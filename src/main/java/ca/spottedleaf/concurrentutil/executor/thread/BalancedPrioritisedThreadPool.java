package ca.spottedleaf.concurrentutil.executor.thread;

import ca.spottedleaf.concurrentutil.executor.PrioritisedExecutor;
import ca.spottedleaf.concurrentutil.executor.queue.PrioritisedTaskQueue;
import ca.spottedleaf.concurrentutil.list.COWArrayList;
import ca.spottedleaf.concurrentutil.util.Priority;
import ca.spottedleaf.concurrentutil.util.TimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

public final class BalancedPrioritisedThreadPool {

    public static final long DEFAULT_GROUP_TIME_SLICE = (long)(15.0e6); // 15ms

    private static final Logger LOGGER = LoggerFactory.getLogger(BalancedPrioritisedThreadPool.class);

    private final Consumer<Thread> threadModifier;

    private final COWArrayList<OrderedStreamGroup> groups = new COWArrayList<>(OrderedStreamGroup.class);
    private final COWArrayList<WorkerThread> threads = new COWArrayList<>(WorkerThread.class);
    private final COWArrayList<WorkerThread> aliveThreads = new COWArrayList<>(WorkerThread.class);

    private final long groupTimeSliceNS;

    private boolean shutdown;

    public BalancedPrioritisedThreadPool(final long groupTimeSliceNS, final Consumer<Thread> threadModifier) {
        this.threadModifier = threadModifier;

        if (threadModifier == null) {
            throw new NullPointerException("Thread factory may not be null");
        }
        this.groupTimeSliceNS = groupTimeSliceNS;
    }

    private void wakeupIdleThread() {
        for (final WorkerThread thread : this.threads.getArray()) {
            if (thread.notifyTasks()) {
                return;
            }
        }
    }

    public Thread[] getAliveThreads() {
        final WorkerThread[] threads = this.aliveThreads.getArray();

        return Arrays.copyOf(threads, threads.length, Thread[].class);
    }

    public Thread[] getCoreThreads() {
        final WorkerThread[] threads = this.threads.getArray();

        return Arrays.copyOf(threads, threads.length, Thread[].class);
    }

    /**
     * Prevents creation of new queues, shutdowns all non-shutdown queues if specified
     */
    public void halt(final boolean shutdownQueues) {
        synchronized (this) {
            this.shutdown = true;
        }

        if (shutdownQueues) {
            for (final OrderedStreamGroup group : this.groups.getArray()) {
                for (final OrderedStreamGroup.Queue queue : group.queues.getArray()) {
                    queue.shutdown();
                }
            }
        }

        for (final WorkerThread thread : this.threads.getArray()) {
            thread.halt(false);
        }
    }

    /**
     * Waits until all threads in this pool have shutdown, or until the specified time has passed.
     * @param msToWait Maximum time to wait.
     * @return {@code false} if the maximum time passed, {@code true} otherwise.
     */
    public boolean join(final long msToWait) {
        try {
            return this.join(msToWait, false);
        } catch (final InterruptedException ex) {
            throw new IllegalStateException(ex);
        }
    }

    /**
     * Waits until all threads in this pool have shutdown, or until the specified time has passed.
     * @param msToWait Maximum time to wait.
     * @return {@code false} if the maximum time passed, {@code true} otherwise.
     * @throws InterruptedException If this thread is interrupted.
     */
    public boolean joinInterruptable(final long msToWait) throws InterruptedException {
        return this.join(msToWait, true);
    }

    private boolean join(final long msToWait, final boolean interruptable) throws InterruptedException {
        final long nsToWait = msToWait * (1000 * 1000);
        final long start = System.nanoTime();
        final long deadline = start + nsToWait;
        boolean interrupted = false;
        try {
            for (final WorkerThread thread : this.aliveThreads.getArray()) {
                while (thread.isAlive()) {
                    final long current = System.nanoTime();
                    if (current - deadline >= 0L && msToWait > 0L) {
                        return false;
                    }

                    try {
                        thread.join(msToWait <= 0L ? 0L : Math.max(1L, (deadline - current) / (1000 * 1000)));
                    } catch (final InterruptedException ex) {
                        if (interruptable) {
                            throw ex;
                        }
                        interrupted = true;
                    }
                }
            }

            return true;
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Shuts down this thread pool, optionally waiting for all tasks to be executed.
     * This function will invoke {@link PrioritisedExecutor#shutdown()} on all created executors on this
     * thread pool.
     * @param wait Whether to wait for tasks to be executed
     */
    public void shutdown(final boolean wait) {
        synchronized (this) {
            this.shutdown = true;
        }

        for (final OrderedStreamGroup group : this.groups.getArray()) {
            for (final OrderedStreamGroup.Queue queue : group.queues.getArray()) {
                queue.shutdown();
            }
        }


        for (final WorkerThread thread : this.threads.getArray()) {
            // none of these can be true or else NPE
            thread.close(false, false);
        }

        if (wait) {
            this.join(0L);
        }
    }

    private void die(final WorkerThread thread) {
        this.aliveThreads.remove(thread);
    }

    public void adjustThreadCount(final int threads) {
        synchronized (this) {
            if (this.shutdown) {
                return;
            }

            final WorkerThread[] currentThreads = this.threads.getArray();
            if (threads == currentThreads.length) {
                // no adjustment needed
                return;
            }

            if (threads < currentThreads.length) {
                // we need to trim threads
                for (int i = 0, difference = currentThreads.length - threads; i < difference; ++i) {
                    final WorkerThread remove = currentThreads[currentThreads.length - i - 1];

                    remove.halt(false);
                    this.threads.remove(remove);
                }
            } else {
                // we need to add threads
                for (int i = 0, difference = threads - currentThreads.length; i < difference; ++i) {
                    final WorkerThread thread = new WorkerThread();

                    this.threadModifier.accept(thread);
                    this.aliveThreads.add(thread);
                    this.threads.add(thread);

                    thread.start();
                }
            }
        }
    }

    public OrderedStreamGroup createOrderedStreamGroup() {
        return this.createOrderedStreamGroup(new AtomicLong());
    }

    public OrderedStreamGroup createOrderedStreamGroup(final AtomicLong subOrderGenerate) {
        synchronized (this) {
            if (this.shutdown) {
                throw new IllegalStateException("Queue is shutdown");
            }

            final OrderedStreamGroup ret = new OrderedStreamGroup(subOrderGenerate);

            this.groups.add(ret);

            return ret;
        }
    }

    private static int compareGroup(final OrderedStreamGroup g1, final OrderedStreamGroup g2) {
        final int parallelismCompare = g1.currentParallelism - g2.currentParallelism;
        if (parallelismCompare != 0) {
            return parallelismCompare;
        }

        return TimeUtil.compareTimes(g1.lastRetrieved, g2.lastRetrieved);
    }

    private List<OrderedStreamGroup> findEligibleGroups() {
        final List<OrderedStreamGroup> nonEmpty = new ArrayList<>();

        for (final OrderedStreamGroup group : this.groups.getArray()) {
            if (group.hasAnyTasks()) {
                nonEmpty.add(group);
            }
        }

        return nonEmpty;
    }

    private OrderedStreamGroup obtainGroup0(final List<OrderedStreamGroup> groups, final long time) {
        OrderedStreamGroup ret = null;

        for (final OrderedStreamGroup group : groups) {
            if (ret == null || compareGroup(group, ret) < 0) {
                ret = group;
            }
        }

        if (ret != null) {
            ret.lastRetrieved = time;
            ++ret.currentParallelism;
            return ret;
        }

        return ret;
    }

    private OrderedStreamGroup obtainGroup(final long time) {
        final List<OrderedStreamGroup> groups = this.findEligibleGroups();

        synchronized (this) {
            return this.obtainGroup0(groups, time);
        }
    }

    private void returnGroup(final OrderedStreamGroup group) {
        synchronized (this) {
            --group.currentParallelism;
        }
    }

    public final class OrderedStreamGroup {

        private final AtomicLong subOrderGenerator;
        private final COWArrayList<Queue> queues = new COWArrayList<>(Queue.class);

        private int currentParallelism;
        private long lastRetrieved = System.nanoTime();

        public OrderedStreamGroup(final AtomicLong subOrderGenerator) {
            this.subOrderGenerator = subOrderGenerator;
        }

        public boolean hasAnyTasks() {
            for (final Queue queue : this.queues.getArray()) {
                // note: we could use hasNoScheduledTasks(); however hasNoScheduledTasks() does not imply that
                //       peekFirst != null which can cause spinning on the shared lock while trying to acquire a group
                if (queue.wrapped.peekFirst() != null) {
                    return true;
                }
            }

            return false;
        }

        public boolean executeTask() {
            for (;;) {
                final PrioritisedExecutor.PrioritisedTask task = this.peekTask();
                if (task == null) {
                    return false;
                }
                if (task.execute()) {
                    return true;
                }
            }
        }

        public PrioritisedExecutor.PrioritisedTask peekTask() {
            PrioritisedExecutor.PrioritisedTask highestTask = null;
            PrioritisedExecutor.PriorityState highestPriority = null;
            for (final Queue wrapper : this.queues.getArray()) {
                final PrioritisedTaskQueue queue = wrapper.wrapped;
                PrioritisedExecutor.PrioritisedTask first;
                PrioritisedExecutor.PriorityState state = null;

                // handle race condition where first entry is executed as we peek it
                // note: entry.getPriorityState() == null implies queue.peekFirst() != entry
                while ((first = queue.peekFirst()) != null && (state = first.getPriorityState()) == null);

                if (first != null) {
                    if (highestPriority == null || state.compareTo(highestPriority) < 0) {
                        highestTask = first;
                        highestPriority = state;
                    }
                } else if (queue.isShutdown() && queue.hasNoScheduledTasks()) {
                    // remove empty shutdown queues
                    this.queues.remove(wrapper);
                }
            }

            return highestTask;
        }

        public Queue createExecutor() {
            synchronized (BalancedPrioritisedThreadPool.this) {
                if (BalancedPrioritisedThreadPool.this.shutdown) {
                    throw new IllegalStateException("Queue is shutdown");
                }

                final Queue ret = new Queue(this.subOrderGenerator);

                this.queues.add(ret);

                return ret;
            }
        }

        public final class Queue implements PrioritisedExecutor {

            private final PrioritisedTaskQueue wrapped;
            private volatile boolean halt;
            private final AtomicLong executors = new AtomicLong();

            public Queue(final AtomicLong subOrderGenerator) {
                this.wrapped = new PrioritisedTaskQueue(subOrderGenerator);
            }

            /**
             * Removes this queue from the thread pool without shutting the queue down or waiting for queued tasks to be executed
             */
            public void halt() {
                this.halt = true;
                OrderedStreamGroup.this.queues.remove(this);
            }

            /**
             * Returns whether this executor is scheduled to run tasks or is running tasks, otherwise it returns whether
             * this queue is not halted and not shutdown.
             */
            public boolean isActive() {
                if (this.halt) {
                    return this.executors.get() > 0L;
                } else {
                    if (!this.isShutdown()) {
                        return true;
                    }

                    return !this.wrapped.hasNoScheduledTasks();
                }
            }

            @Override
            public long getTotalTasksScheduled() {
                return this.wrapped.getTotalTasksScheduled();
            }

            @Override
            public long getTotalTasksExecuted() {
                return this.wrapped.getTotalTasksExecuted();
            }

            @Override
            public long generateNextSubOrder() {
                return this.wrapped.generateNextSubOrder();
            }

            @Override
            public boolean shutdown() {
                return this.wrapped.shutdown();
            }

            @Override
            public boolean isShutdown() {
                return this.wrapped.isShutdown();
            }

            @Override
            public PrioritisedTask createTask(final Runnable task) {
                return this.createTask(task, Priority.NORMAL);
            }

            @Override
            public PrioritisedTask createTask(final Runnable task, final Priority priority) {
                return this.createTask(task, priority, this.generateNextSubOrder(), 0L);
            }

            @Override
            public PrioritisedTask createTask(final Runnable task, final Priority priority, final long subOrder, final long stream) {
                return new Task(this.wrapped.createTask(() -> {
                    Queue.this.executors.getAndIncrement();
                    try {
                        task.run();
                    } finally {
                        Queue.this.executors.getAndDecrement();
                    }
                }, priority, subOrder, stream));
            }

            @Override
            public PrioritisedTask queueTask(final Runnable task) {
                final PrioritisedTask ret = this.createTask(task);
                ret.queue();
                return ret;
            }

            @Override
            public PrioritisedTask queueTask(final Runnable task, final Priority priority) {
                final PrioritisedTask ret = this.createTask(task, priority);
                ret.queue();
                return ret;
            }

            @Override
            public PrioritisedTask queueTask(final Runnable task, final Priority priority, final long subOrder, final long stream) {
                final PrioritisedTask ret = this.createTask(task, priority, subOrder, stream);
                ret.queue();
                return ret;
            }

            @Override
            public boolean executeTask() {
                return this.wrapped.executeTask();
            }

            private final class Task implements PrioritisedTask {

                private final PrioritisedTask wrap;

                public Task(final PrioritisedTask wrap) {
                    this.wrap = wrap;
                }

                @Override
                public PrioritisedExecutor getExecutor() {
                    return Queue.this;
                }

                @Override
                public boolean queue() {
                    if (this.wrap.queue()) {
                        BalancedPrioritisedThreadPool.this.wakeupIdleThread();
                        return true;
                    }
                    return false;
                }

                @Override
                public boolean isQueued() {
                    return this.wrap.isQueued();
                }

                @Override
                public boolean cancel() {
                    return this.wrap.cancel();
                }

                @Override
                public boolean execute() {
                    return this.wrap.execute();
                }

                @Override
                public Priority getPriority() {
                    return this.wrap.getPriority();
                }

                @Override
                public boolean setPriority(final Priority priority) {
                    return this.wrap.setPriority(priority);
                }

                @Override
                public boolean raisePriority(final Priority priority) {
                    return this.wrap.raisePriority(priority);
                }

                @Override
                public boolean lowerPriority(final Priority priority) {
                    return this.wrap.lowerPriority(priority);
                }

                @Override
                public long getSubOrder() {
                    return this.wrap.getSubOrder();
                }

                @Override
                public boolean setSubOrder(final long subOrder) {
                    return this.wrap.setSubOrder(subOrder);
                }

                @Override
                public boolean raiseSubOrder(final long subOrder) {
                    return this.wrap.raiseSubOrder(subOrder);
                }

                @Override
                public boolean lowerSubOrder(final long subOrder) {
                    return this.wrap.lowerSubOrder(subOrder);
                }

                @Override
                public long getStream() {
                    return this.wrap.getStream();
                }

                @Override
                public boolean setStream(final long stream) {
                    return this.wrap.setStream(stream);
                }

                @Override
                public boolean setPrioritySubOrderStream(final Priority priority, final long subOrder, final long stream) {
                    return this.wrap.setPrioritySubOrderStream(priority, subOrder, stream);
                }

                @Override
                public PriorityState getPriorityState() {
                    return this.wrap.getPriorityState();
                }
            }
        }
    }

    private final class WorkerThread extends PrioritisedQueueExecutorThread {

        public WorkerThread() {
            super(null);
        }

        @Override
        protected void die() {
            BalancedPrioritisedThreadPool.this.die(this);
        }

        @Override
        protected boolean pollTasks() {
            boolean ret = false;

            for (;;) {
                if (this.halted) {
                    break;
                }

                final OrderedStreamGroup group = BalancedPrioritisedThreadPool.this.obtainGroup(System.nanoTime());
                if (group == null) {
                    break;
                }
                final long deadline = System.nanoTime() + BalancedPrioritisedThreadPool.this.groupTimeSliceNS;
                do {
                    try {
                        if (this.halted) {
                            break;
                        }
                        if (!group.executeTask()) {
                            // no more tasks, try next group
                            break;
                        }
                        ret = true;
                    } catch (final Throwable throwable) {
                        LOGGER.error("Exception thrown from thread '" + this.getName(), throwable);
                    }
                } while (System.nanoTime() - deadline <= 0L);

                BalancedPrioritisedThreadPool.this.returnGroup(group);
            }


            return ret;
        }
    }
}
