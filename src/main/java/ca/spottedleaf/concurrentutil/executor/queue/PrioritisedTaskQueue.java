package ca.spottedleaf.concurrentutil.executor.queue;

import ca.spottedleaf.concurrentutil.executor.PrioritisedExecutor;
import ca.spottedleaf.concurrentutil.util.ConcurrentUtil;
import ca.spottedleaf.concurrentutil.util.Priority;
import java.lang.invoke.VarHandle;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

public final class PrioritisedTaskQueue implements PrioritisedExecutor {

    /**
     * Whether to order tasks by (lower) stream id after (higher) priority
     */
    public static final long FLAG_ORDER_BY_STREAM = 1L << 0;

    /**
     * Required for tie-breaking in the queue
     */
    private final AtomicLong taskIdGenerator = new AtomicLong();
    private final AtomicLong scheduledTasks = new AtomicLong();
    private final AtomicLong executedTasks = new AtomicLong();
    private final AtomicLong subOrderGenerator;
    private final AtomicBoolean shutdown = new AtomicBoolean();
    private final ConcurrentSkipListMap<PrioritisedQueuedTask.Holder, Boolean> tasks;
    private final Consumer<PrioritisedTask> queueHook;

    public PrioritisedTaskQueue() {
        this(new AtomicLong());
    }

    public PrioritisedTaskQueue(final AtomicLong subOrderGenerator) {
        this(subOrderGenerator, 0L);
    }

    public PrioritisedTaskQueue(final AtomicLong subOrderGenerator, final long flags) {
        this(subOrderGenerator, flags, null);
    }

    public PrioritisedTaskQueue(final AtomicLong subOrderGenerator, final long flags, final Consumer<PrioritisedTask> queueHook) {
        this.subOrderGenerator = subOrderGenerator;
        this.tasks = new ConcurrentSkipListMap<>(((flags & FLAG_ORDER_BY_STREAM) != 0L) ? PrioritisedQueuedTask.COMPARATOR_STREAM : PrioritisedQueuedTask.COMPARATOR);
        this.queueHook = queueHook;
    }

    @Override
    public long getTotalTasksScheduled() {
        return this.scheduledTasks.get();
    }

    @Override
    public long getTotalTasksExecuted() {
        return this.executedTasks.get();
    }

    @Override
    public long generateNextSubOrder() {
        return this.subOrderGenerator.getAndIncrement();
    }

    @Override
    public boolean shutdown() {
        return this.shutdown.compareAndSet(false, true);
    }

    @Override
    public boolean isShutdown() {
        return this.shutdown.get();
    }

    public PrioritisedTask peekFirst() {
        final Map.Entry<PrioritisedQueuedTask.Holder, Boolean> firstEntry = this.tasks.firstEntry();
        return firstEntry == null ? null : firstEntry.getKey().task;
    }

    public Priority getHighestPriority() {
        final Map.Entry<PrioritisedQueuedTask.Holder, Boolean> firstEntry = this.tasks.firstEntry();
        return firstEntry == null ? null : Priority.getPriority(firstEntry.getKey().priority);
    }

    public boolean hasNoScheduledTasks() {
        final long executedTasks = this.executedTasks.get();
        final long scheduledTasks = this.scheduledTasks.get();

        return executedTasks == scheduledTasks;
    }

    public PriorityState getHighestPriorityState() {
        final Map.Entry<PrioritisedQueuedTask.Holder, Boolean> firstEntry = this.tasks.firstEntry();
        if (firstEntry == null) {
            return null;
        }

        final PrioritisedQueuedTask.Holder holder = firstEntry.getKey();

        return new PriorityState(Priority.getPriority(holder.priority), holder.subOrder, holder.stream);
    }

    public Runnable pollTask() {
        for (;;) {
            final Map.Entry<PrioritisedQueuedTask.Holder, Boolean> firstEntry = this.tasks.pollFirstEntry();
            if (firstEntry != null) {
                final PrioritisedQueuedTask.Holder task = firstEntry.getKey();
                task.markRemoved();
                if (!task.task.cancel()) {
                    continue;
                }
                return task.task.execute;
            }

            return null;
        }
    }

    @Override
    public boolean executeTask() {
        for (;;) {
            final Map.Entry<PrioritisedQueuedTask.Holder, Boolean> firstEntry = this.tasks.pollFirstEntry();
            if (firstEntry != null) {
                final PrioritisedQueuedTask.Holder task = firstEntry.getKey();
                task.markRemoved();
                if (!task.task.execute()) {
                    continue;
                }
                return true;
            }

            return false;
        }
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
    public PrioritisedTask createTask(final Runnable task, final Priority priority, final long subOrder,
                                      final long stream) {
        if (!Priority.isValidPriority(priority)) {
            throw new IllegalArgumentException("Invalid priority " + priority);
        }

        return new PrioritisedQueuedTask(task, priority, subOrder, stream, this.taskIdGenerator.getAndIncrement());
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
    public PrioritisedTask queueTask(final Runnable task, final Priority priority, final long subOrder,
                                     final long stream) {
        final PrioritisedTask ret = this.createTask(task, priority, subOrder, stream);

        ret.queue();

        return ret;
    }

    private final class PrioritisedQueuedTask implements PrioritisedExecutor.PrioritisedTask {
        public static final Comparator<PrioritisedQueuedTask.Holder> COMPARATOR = (final PrioritisedQueuedTask.Holder t1, final PrioritisedQueuedTask.Holder t2) -> {
            final int priorityCompare = t1.priority - t2.priority;
            if (priorityCompare != 0) {
                return priorityCompare;
            }

            final int subOrderCompare = Long.compare(t1.subOrder, t2.subOrder);
            if (subOrderCompare != 0) {
                return subOrderCompare;
            }

            return Long.signum(t1.id - t2.id);
        };

        public static final Comparator<PrioritisedQueuedTask.Holder> COMPARATOR_STREAM = (final PrioritisedQueuedTask.Holder t1, final PrioritisedQueuedTask.Holder t2) -> {
            final int priorityCompare = t1.priority - t2.priority;
            if (priorityCompare != 0) {
                return priorityCompare;
            }

            final int streamCompare = Long.compare(t1.stream, t2.stream);
            if (streamCompare != 0) {
                return streamCompare;
            }

            final int subOrderCompare = Long.compare(t1.subOrder, t2.subOrder);
            if (subOrderCompare != 0) {
                return subOrderCompare;
            }

            return Long.signum(t1.id - t2.id);
        };

        private static final class Holder {
            private final PrioritisedQueuedTask task;
            private final int priority;
            private final long subOrder;
            private final long stream;
            private final long id;

            private volatile boolean removed;
            private static final VarHandle REMOVED_HANDLE = ConcurrentUtil.getVarHandle(Holder.class, "removed", boolean.class);

            private Holder(final PrioritisedQueuedTask task, final int priority, final long subOrder, final long stream,
                           final long id) {
                this.task = task;
                this.priority = priority;
                this.subOrder = subOrder;
                this.stream = stream;
                this.id = id;
            }

            /**
             * Returns true if marked as removed
             */
            public boolean markRemoved() {
                return false == (boolean)REMOVED_HANDLE.compareAndExchange((Holder)this, (boolean)false, (boolean)true);
            }
        }

        private final long id;
        private final Runnable execute;

        private Priority priority;
        private long subOrder;
        private long stream;
        private Holder holder;

        public PrioritisedQueuedTask(final Runnable execute, final Priority priority, final long subOrder,
                                     final long stream, final long id) {
            this.execute = execute;
            this.priority = priority;
            this.subOrder = subOrder;
            this.stream = stream;
            this.id = id;
        }

        @Override
        public PrioritisedExecutor getExecutor() {
            return PrioritisedTaskQueue.this;
        }

        @Override
        public boolean queue() {
            synchronized (this) {
                if (this.holder != null || this.priority == Priority.COMPLETING) {
                    return false;
                }

                if (PrioritisedTaskQueue.this.isShutdown()) {
                    throw new IllegalStateException("Queue is shutdown");
                }

                final Holder holder = new Holder(this, this.priority.priority, this.subOrder, this.stream, this.id);
                this.holder = holder;

                PrioritisedTaskQueue.this.scheduledTasks.getAndIncrement();
                PrioritisedTaskQueue.this.tasks.put(holder, Boolean.TRUE);
            }

            if (PrioritisedTaskQueue.this.isShutdown()) {
                if (this.cancel()) {
                    throw new IllegalStateException("Queue is shutdown");
                }
            }

            final Consumer<PrioritisedTask> queueHook = PrioritisedTaskQueue.this.queueHook;
            if (queueHook != null) {
                queueHook.accept(this);
            }

            return true;
        }

        @Override
        public boolean isQueued() {
            synchronized (this) {
                return this.holder != null && this.priority != Priority.COMPLETING;
            }
        }

        @Override
        public boolean cancel() {
            synchronized (this) {
                if (this.priority == Priority.COMPLETING) {
                    return false;
                }

                this.priority = Priority.COMPLETING;

                if (this.holder != null) {
                    if (this.holder.markRemoved()) {
                        PrioritisedTaskQueue.this.tasks.remove(this.holder);
                    }
                    PrioritisedTaskQueue.this.executedTasks.getAndIncrement();
                }

                return true;
            }
        }

        @Override
        public boolean execute() {
            final boolean increaseExecuted;

            synchronized (this) {
                if (this.priority == Priority.COMPLETING) {
                    return false;
                }

                this.priority = Priority.COMPLETING;

                if (increaseExecuted = (this.holder != null)) {
                    if (this.holder.markRemoved()) {
                        PrioritisedTaskQueue.this.tasks.remove(this.holder);
                    }
                }
            }

            try {
                this.execute.run();
                return true;
            } finally {
                if (increaseExecuted) {
                    PrioritisedTaskQueue.this.executedTasks.getAndIncrement();
                }
            }
        }

        @Override
        public Priority getPriority() {
            synchronized (this) {
                return this.priority;
            }
        }

        private void updateHolder(final int priority, final long subOrder, final long stream, final long id) {
            final Holder oldHolder = this.holder;
            if (oldHolder == null) {
                return;
            }

            final Holder newHolder = new Holder(this, priority, subOrder, stream, id);
            this.holder = newHolder;

            PrioritisedTaskQueue.this.tasks.put(newHolder, Boolean.TRUE);

            if (oldHolder.markRemoved()) {
                PrioritisedTaskQueue.this.tasks.remove(oldHolder);
            }
        }

        @Override
        public boolean setPriority(final Priority priority) {
            synchronized (this) {
                if (this.priority == Priority.COMPLETING || this.priority == priority) {
                    return false;
                }

                this.priority = priority;
                this.updateHolder(priority.priority, this.subOrder, this.stream, this.id);

                return true;
            }
        }

        @Override
        public boolean raisePriority(final Priority priority) {
            synchronized (this) {
                if (this.priority == Priority.COMPLETING || this.priority.isHigherOrEqualPriority(priority)) {
                    return false;
                }

                this.priority = priority;
                this.updateHolder(priority.priority, this.subOrder, this.stream, this.id);

                return true;
            }
        }

        @Override
        public boolean lowerPriority(Priority priority) {
            synchronized (this) {
                if (this.priority == Priority.COMPLETING || this.priority.isLowerOrEqualPriority(priority)) {
                    return false;
                }

                this.priority = priority;
                this.updateHolder(priority.priority, this.subOrder, this.stream, this.id);

                return true;
            }
        }

        @Override
        public long getSubOrder() {
            synchronized (this) {
                return this.priority == Priority.COMPLETING ? 0L : this.subOrder;
            }
        }

        @Override
        public boolean setSubOrder(final long subOrder) {
            synchronized (this) {
                if (this.priority == Priority.COMPLETING || this.subOrder == subOrder) {
                    return false;
                }

                this.subOrder = subOrder;
                this.updateHolder(this.priority.priority, subOrder, this.stream, this.id);

                return true;
            }
        }

        @Override
        public boolean raiseSubOrder(long subOrder) {
            synchronized (this) {
                if (this.priority == Priority.COMPLETING || this.subOrder >= subOrder) {
                    return false;
                }

                this.subOrder = subOrder;
                this.updateHolder(this.priority.priority, subOrder, this.stream, this.id);

                return true;
            }
        }

        @Override
        public boolean lowerSubOrder(final long subOrder) {
            synchronized (this) {
                if (this.priority == Priority.COMPLETING || this.subOrder <= subOrder) {
                    return false;
                }

                this.subOrder = subOrder;
                this.updateHolder(this.priority.priority, subOrder, this.stream, this.id);

                return true;
            }
        }

        @Override
        public long getStream() {
            synchronized (this) {
                return this.priority == Priority.COMPLETING ? 0L : this.stream;
            }
        }

        @Override
        public boolean setStream(final long stream) {
            synchronized (this) {
                if (this.priority == Priority.COMPLETING || this.stream == stream) {
                    return false;
                }

                this.stream = stream;
                this.updateHolder(this.priority.priority, this.subOrder, stream, this.id);

                return true;
            }
        }

        @Override
        public boolean setPrioritySubOrderStream(final Priority priority, final long subOrder, final long stream) {
            synchronized (this) {
                if (this.priority == Priority.COMPLETING
                        || (this.priority == priority && this.subOrder == subOrder && this.stream == stream)) {
                    return false;
                }

                this.priority = priority;
                this.subOrder = subOrder;
                this.stream = stream;
                this.updateHolder(priority.priority, subOrder, stream, this.id);

                return true;
            }
        }

        @Override
        public PriorityState getPriorityState() {
            synchronized (this) {
                if (this.priority ==  Priority.COMPLETING) {
                    return null;
                }

                return new PriorityState(this.priority, this.subOrder, this.stream);
            }
        }
    }
}
