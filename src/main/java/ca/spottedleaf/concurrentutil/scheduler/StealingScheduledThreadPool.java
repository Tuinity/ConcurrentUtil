package ca.spottedleaf.concurrentutil.scheduler;

import ca.spottedleaf.concurrentutil.list.COWArrayList;
import ca.spottedleaf.concurrentutil.numa.OSNuma;
import ca.spottedleaf.concurrentutil.util.ConcurrentUtil;
import ca.spottedleaf.concurrentutil.util.LazyRunnable;
import ca.spottedleaf.concurrentutil.util.TimeUtil;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.objects.ReferenceOpenHashSet;
import java.lang.invoke.VarHandle;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;

public final class StealingScheduledThreadPool extends Scheduler {

    /**
     * Schedules new tasks to the threads which are loaded the least.
     */
    public static final long FLAG_SCHEDULE_EVENLY = 1L << 0;

    /**
     * Schedules new tasks to the nearest threads for the thread scheduling the task.
     */
    public static final long FLAG_SCHEDULE_NEAR   = 1L << 1;

    public static final long DEFAULT_FLAGS = FLAG_SCHEDULE_EVENLY;

    private final ThreadFactory threadFactory;
    private final OSNuma numa;

    private final COWArrayList<TickThreadRunner> coreThreads = new COWArrayList<>(TickThreadRunner.class);
    private final COWArrayList<TickThreadRunner> aliveThreads = new COWArrayList<>(TickThreadRunner.class);

    private final COWArrayList<NodeThreads> nodes = new COWArrayList<>(NodeThreads.class);

    // used when adjusting threads
    private final ReferenceOpenHashSet<SchedulableTick> allTasks = new ReferenceOpenHashSet<>();

    private boolean shutdown;

    private long stealThresholdNS;
    private long taskTimeSliceNS;
    private long flags;

    public StealingScheduledThreadPool(final ThreadFactory threadFactory, final OSNuma numa) {
        this.threadFactory = threadFactory;
        this.numa = numa;

        if (threadFactory == null) {
            throw new NullPointerException("Null thread factory");
        }

        this.setFlags(DEFAULT_FLAGS);
    }

    public OSNuma getNuma() {
        return this.numa;
    }

    private static ScheduledState getState(final SchedulableTick tick) {
        return (ScheduledState)tick.getState();
    }

    private static Thread[] getThreads(final COWArrayList<TickThreadRunner> runners) {
        final TickThreadRunner[] array = runners.getArray();
        final Thread[] ret = new Thread[array.length];

        for (int i = 0; i < array.length; ++i) {
            ret[i] = array[i].thread;
        }

        return ret;
    }

    @Override
    public Thread[] getAliveThreads() {
        return getThreads(this.aliveThreads);
    }

    @Override
    public Thread[] getCoreThreads() {
        return getThreads(this.coreThreads);
    }

    public synchronized boolean isShutdown() {
        return this.shutdown;
    }

    public synchronized boolean setThreadAllocation(final Int2IntMap threadsPerNode, final long stealThresholdNS,
                                                    final long taskTimeSliceNS) {
        if (this.shutdown) {
            return false;
        }

        this.stealThresholdNS = stealThresholdNS;
        this.taskTimeSliceNS = taskTimeSliceNS;

        final NodeThreads[] nodes = new NodeThreads[threadsPerNode.size()];
        final List<TickThreadRunner> newRunners = new ArrayList<>();

        int nodesIndex = 0;
        for (final Int2IntMap.Entry entry : threadsPerNode.int2IntEntrySet()) {
            final int node = entry.getIntKey();
            final int threads = entry.getIntValue();

            final TickThreadRunner[] runners = new TickThreadRunner[threads];
            final NodeThreads nodeThreads = new NodeThreads(runners, node);

            for (int i = 0; i < threads; ++i) {
                final LazyRunnable run = new LazyRunnable();

                run.setRunnable(runners[i] = new TickThreadRunner(this.threadFactory.newThread(run), this, nodeThreads, nodes));
                newRunners.add(runners[i]);
                this.aliveThreads.add(runners[i]);
            }

            nodes[nodesIndex++] = nodeThreads;
        }

        // swap the threads
        final LongOpenHashSet oldRunners = new LongOpenHashSet(this.coreThreads.getArray().length);
        for (final TickThreadRunner oldRunner : this.coreThreads.getArray()) {
            oldRunners.add(oldRunner.id);
            oldRunner.halt();
        }
        this.coreThreads.set(newRunners.toArray(new TickThreadRunner[0]));
        this.nodes.set(nodes);

        final SchedulableTick[] allTasks;
        synchronized (this.allTasks) {
            allTasks = this.allTasks.toArray(new SchedulableTick[0]);
        }

        for (final SchedulableTick tick : allTasks) {
            final ScheduledState state = getState(tick);
            final TickThreadRunner runner = state.getOwnedBy();

            if (runner == null || oldRunners.contains(runner.id)) {
                final TickThreadRunner newRunner = this.selectRunner(runner, this.nodes.getArray());

                if (newRunner == runner) {
                    // both should be null
                    if (runner != null) {
                        throw new IllegalStateException();
                    }
                    continue;
                }

                state.transferScheduling(runner, newRunner);
            }
        }

        // start new threads
        if (this.numa.isAvailable()) {
            final long[] prevAffinity = this.numa.getCurrentThreadAffinity();
            try {
                for (final NodeThreads node : this.nodes.getArray()) {
                    this.numa.setCurrentNumaAffinity(new int[] { node.nodeNumber });
                    for (final TickThreadRunner runner : node.threads) {
                        runner.thread.start();
                    }
                }
            } finally {
                this.numa.setCurrentThreadAffinity(prevAffinity);
            }
        } else {
            for (final TickThreadRunner runner : this.coreThreads.getArray()) {
                runner.thread.start();
            }
        }

        return true;
    }

    public synchronized boolean setFlags(final long flags) {
        if (this.shutdown) {
            return false;
        }

        this.flags = flags;

        return true;
    }

    private void die(final TickThreadRunner runner) {
        synchronized (this) {
            this.aliveThreads.remove(runner);
        }
    }

    @Override
    public void halt() {
        synchronized (this) {
            this.shutdown = true;
        }

        for (final TickThreadRunner runner : this.coreThreads.getArray()) {
            runner.halt();
        }
    }

    @Override
    public boolean join(final long msToWait) {
        try {
            return this.join(msToWait, false);
        } catch (final InterruptedException ex) {
            throw new IllegalStateException(ex);
        }
    }

    @Override
    public boolean joinInterruptable(final long msToWait) throws InterruptedException {
        return this.join(msToWait, true);
    }

    private boolean join(final long msToWait, final boolean interruptable) throws InterruptedException {
        synchronized (this) {
            if (!this.shutdown) {
                throw new IllegalStateException("Attempting to join on non-shutdown pool");
            }
        }

        final long nsToWait = TimeUnit.MILLISECONDS.toNanos(msToWait);
        final long start = System.nanoTime();
        final long deadline = start + nsToWait;
        boolean interrupted = false;
        try {
            for (final TickThreadRunner runner : this.aliveThreads.getArray()) {
                final Thread thread = runner.thread;
                while (thread.isAlive()) {
                    try {
                        if (msToWait > 0L) {
                            final long current = System.nanoTime();
                            if (current - deadline >= 0L) {
                                return false;
                            }
                            thread.join(Duration.ofNanos(deadline - current));
                        } else {
                            thread.join();
                        }
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

    private TickThreadRunner selectRunner(final TickThreadRunner previousRunner, final NodeThreads[] nodes) {
        final int currentNode = previousRunner == null ? this.numa.getCurrentNumaNode() : previousRunner.node.nodeNumber;

        if ((this.flags & FLAG_SCHEDULE_NEAR) != 0L) {
            TickThreadRunner selected = null;
            int selectedDistance = Integer.MAX_VALUE;
            int selectedSize = Integer.MAX_VALUE;

            for (final NodeThreads node : nodes) {
                final int rawDistance = this.numa.getNumaDistance(currentNode, node.nodeNumber);
                final int distance = rawDistance <= 0 ? Integer.MAX_VALUE : rawDistance;
                if (distance > selectedDistance) {
                    continue;
                }
                for (final TickThreadRunner runner : node.threads) {
                    // yes the size is just a rough guess...
                    final int size = runner.tickQueue.size();
                    // break ties on distance with size
                    if (distance < selectedDistance || size < selectedSize) {
                        // note: distance <= selectedDistance here
                        selected = runner;
                        selectedDistance = distance;
                        selectedSize = size;
                    }
                }
            }

            return selected;
        } else {
            TickThreadRunner selected = null;
            int selectedSize = Integer.MAX_VALUE;
            int selectedDistance = Integer.MAX_VALUE;

            for (final NodeThreads node : nodes) {
                final int rawDistance = this.numa.getNumaDistance(currentNode, node.nodeNumber);
                final int distance = rawDistance <= 0 ? Integer.MAX_VALUE : rawDistance;
                for (final TickThreadRunner runner : node.threads) {
                    // yes the size is just a rough guess...
                    final int size = runner.tickQueue.size();
                    // break ties on size with numa distance
                    if (size < selectedSize || (size == selectedSize && distance < selectedDistance)) {
                        selected = runner;
                        selectedSize = size;
                        selectedDistance = distance;
                    }
                }
            }

            return selected;
        }
    }

    @Override
    public void schedule(final SchedulableTick tick) {
        NodeThreads[] nodes = this.nodes.getArray();

        final ScheduledState state = new ScheduledState(tick);

        if (!tick.setState(state)) {
            throw new IllegalStateException("Task is already scheduled");
        }

        final TickThreadRunner initialRunner = this.selectRunner(null, nodes);

        if (!state.initScheduling(this, initialRunner)) {
            throw new IllegalStateException("Task is already scheduled");
        }

        // note: the task is now in the task set
        //       it may be possible that the runner we selected was halted while scheduling
        //       we can check for this by seeing if the nodes array changed

        if (nodes != (nodes = this.nodes.getArray())) {
            // try to re-assign the runner
            // we do not need to repeat this if it fails, as the task is in the task set and would be
            // handled by any future thread allocation change directly
            state.transferScheduling(initialRunner, this.selectRunner(null, nodes));
        }

        // intermediate tasks are not handled during scheduling init
        if (tick.hasTasks()) {
            this.notifyTasks(tick);
        }
    }

    @Override
    public void notifyTasks(final SchedulableTick tick) {
        if (tick.getState() instanceof ScheduledState state) {
            state.scheduleTasks();
        }
    }

    @Override
    public boolean cancel(final SchedulableTick tick) {
        if (tick.getState() instanceof ScheduledState state) {
            return state.tryCancel();
        } else {
            return false;
        }
    }

    // note: holds the tick's scheduled lock
    private void tickAdded(final ScheduledState tick) {
        synchronized (this.allTasks) {
            this.allTasks.add(tick.tick);
        }
    }

    // note: holds the tick's scheduled lock
    private void tickCancelled(final ScheduledState tick) {
        synchronized (this.allTasks) {
            this.allTasks.remove(tick.tick);
        }
    }

    private static final class ScheduledState {
        private final SchedulableTick tick;

        private StealingScheduledThreadPool scheduledTo;
        private TickThreadRunner ownedBy;

        private TickScheduleHolder tickScheduleHolder;
        private TaskScheduleHolder taskScheduleHolder;

        private static final int STATE_UNSCHEDULED       = 1 << 0;
        // scheduled but no intermediate tasks
        private static final int STATE_IDLE              = 1 << 1;
        // scheduled but has intermediate tasks
        private static final int STATE_HAS_TASKS         = 1 << 2;
        // running intermediate tasks or tick
        private static final int STATE_RUNNING           = 1 << 3;
        private static final int STATE_RUNNING_CANCELLED = 1 << 4;
        private static final int STATE_CANCELLED         = 1 << 5;

        private volatile int state = STATE_UNSCHEDULED;

        private ScheduledState(final SchedulableTick tick) {
            this.tick = tick;
        }

        private TickThreadRunner getOwnedBy() {
            synchronized (this) {
                return this.ownedBy;
            }
        }

        private boolean initScheduling(final StealingScheduledThreadPool threadPool, final TickThreadRunner runner) {
            synchronized (this) {
                if (this.state != STATE_UNSCHEDULED) {
                    return false;
                }

                final long scheduledStart = this.tick.scheduledStart;
                if (scheduledStart == TimeUtil.DEADLINE_NOT_SET) {
                    throw new IllegalStateException("Start must be set when scheduling");
                }

                threadPool.tickAdded(this);

                this.scheduledTo = threadPool;
                if (runner != null) {
                    this.ownedBy = runner;

                    this.tickScheduleHolder = new TickScheduleHolder(this, scheduledStart, false);
                    runner.tickQueue.put(this.tickScheduleHolder, Boolean.TRUE);
                    if (runner.findFirstTick() == this.tickScheduleHolder) {
                        runner.interruptIfIdle();
                    }
                }

                this.state = STATE_IDLE;

                return true;
            }
        }

        private boolean transferScheduling(final TickThreadRunner from, final TickThreadRunner to) {
            synchronized (this) {
                if (this.ownedBy != from) {
                    return false;
                }

                switch (this.state) {
                    case STATE_UNSCHEDULED: {
                        throw new IllegalStateException();
                    }

                    case STATE_IDLE: {
                        this.ownedBy = to;

                        // we need to re-schedule the tick task
                        this.reschedule(false);

                        if (this.taskScheduleHolder != null) {
                            throw new IllegalStateException();
                        }

                        return true;
                    }
                    case STATE_HAS_TASKS: {
                        this.ownedBy = to;

                        // we need to re-schedule the tick task
                        this.reschedule(false);

                        // re-schedule intermediate task if applicable
                        if (to == null) {
                            this.taskScheduleHolder = null;
                        } else {
                            if (this.taskScheduleHolder != null) {
                                this.taskScheduleHolder = new TaskScheduleHolder(this, this.taskScheduleHolder.lastDrainedTasks);
                                to.taskQueue.put(this.taskScheduleHolder, Boolean.TRUE);
                                to.interruptIfIdle();
                            }
                        }

                        return true;
                    }

                    case STATE_RUNNING: {
                        this.ownedBy = to;

                        // these will re-schedule these after the previous owner is done
                        this.tickScheduleHolder = null;
                        this.taskScheduleHolder = null;
                        return true;
                    }

                    case STATE_RUNNING_CANCELLED:
                    case STATE_CANCELLED: {
                        return false;
                    }

                    default: {
                        throw new IllegalStateException("Unknown state: " + this.state);
                    }
                }
            }
        }

        private boolean trySteal(final TickThreadRunner from, final TickThreadRunner to) {
            synchronized (this) {
                if (this.ownedBy != from) {
                    return false;
                }

                // can only steal tasks idling or awaiting task execution

                switch (this.state) {
                    case STATE_UNSCHEDULED: {
                        throw new IllegalStateException();
                    }

                    case STATE_IDLE: {
                        this.ownedBy = to;

                        // we need to re-schedule the tick task
                        this.reschedule(true);

                        if (this.taskScheduleHolder != null) {
                            throw new IllegalStateException();
                        }

                        return true;
                    }
                    case STATE_HAS_TASKS: {
                        this.ownedBy = to;

                        // we need to re-schedule the tick task
                        this.reschedule(true);

                        // re-schedule intermediate task if applicable
                        if (to == null) {
                            this.taskScheduleHolder = null;
                        } else {
                            if (this.taskScheduleHolder != null) {
                                this.taskScheduleHolder = new TaskScheduleHolder(this, this.taskScheduleHolder.lastDrainedTasks);
                                to.taskQueue.put(this.taskScheduleHolder, Boolean.TRUE);
                                to.interruptIfIdle();
                            }
                        }

                        return true;
                    }

                    case STATE_RUNNING:
                    case STATE_RUNNING_CANCELLED:
                    case STATE_CANCELLED: {
                        return false;
                    }

                    default: {
                        throw new IllegalStateException("Unknown state: " + this.state);
                    }
                }
            }
        }

        private void scheduleTasks() {
            final int currState = this.state;
            if (currState != STATE_IDLE) {
                // try to avoid acquiring lock unless we need to
                // unscheduled, has tasks, is running, or is cancelled
                return;
            }

            final long now = System.nanoTime();

            synchronized (this) {
                if (this.state != STATE_IDLE) {
                    return;
                }

                this.taskScheduleHolder = new TaskScheduleHolder(this, now);
                if (this.ownedBy != null) {
                    this.ownedBy.taskQueue.put(this.taskScheduleHolder, Boolean.TRUE);
                    if (Thread.currentThread() != this.ownedBy.thread) {
                        this.ownedBy.interruptIfIdle();
                    }
                }

                this.state = STATE_HAS_TASKS;
            }
        }

        // holds schedule lock
        private void reschedule(final boolean stolen) {
            final long scheduledStart = this.tick.scheduledStart;
            if (scheduledStart == TimeUtil.DEADLINE_NOT_SET) {
                throw new IllegalStateException("Start must be set when scheduling");
            }
            if (this.ownedBy != null) {
                this.tickScheduleHolder = new TickScheduleHolder(this, scheduledStart, stolen);
                this.ownedBy.tickQueue.put(this.tickScheduleHolder, Boolean.TRUE);
                if (Thread.currentThread() != this.ownedBy.thread && this.ownedBy.findFirstTick() == this.tickScheduleHolder) {
                    this.ownedBy.tryInterrupt();
                }
            }
        }

        private boolean trySetRunning(final TickThreadRunner expectedOwner) {
            synchronized (this) {
                if (this.ownedBy != expectedOwner) {
                    return false;
                }

                switch (this.state) {
                    case STATE_UNSCHEDULED: {
                        throw new IllegalStateException("Cannot be unscheduled here");
                    }
                    case STATE_IDLE:
                    case STATE_HAS_TASKS: {
                        this.state = STATE_RUNNING;
                        return true;
                    }

                    case STATE_RUNNING:
                    case STATE_RUNNING_CANCELLED:
                    case STATE_CANCELLED: {
                        return false;
                    }

                    default: {
                        throw new IllegalStateException("Unknown state: " + this.state);
                    }
                }
            }
        }

        private boolean tryCancel() {
            synchronized (this) {
                switch (this.state) {
                    case STATE_UNSCHEDULED: {
                        return false;
                    }
                    case STATE_IDLE:
                    case STATE_HAS_TASKS: {
                        this.doCancel();
                        return true;
                    }

                    case STATE_RUNNING: {
                        this.state = STATE_RUNNING_CANCELLED;
                        return true;
                    }

                    case STATE_RUNNING_CANCELLED:
                    case STATE_CANCELLED: {
                        return false;
                    }

                    default: {
                        throw new IllegalStateException("Unknown state: " + this.state);
                    }
                }
            }
        }

        // only run if trySetRunning() returns true
        private void finishTaskExecution(final boolean cancel) {
            synchronized (this) {
                if (this.state != STATE_RUNNING && this.state != STATE_RUNNING_CANCELLED) {
                    throw new IllegalStateException("Must be running here");
                }

                if (cancel || this.state == STATE_RUNNING_CANCELLED) {
                    this.doCancel();
                    return;
                } else {
                    this.taskScheduleHolder = null;
                    if (this.tickScheduleHolder == null) {
                        // this task was stolen while running tasks, and as a result was not scheduled to its new thread
                        this.reschedule(false);
                    }
                    this.state = STATE_IDLE;
                    // note: expect caller to invoke notifyTasks if there are still more tasks
                }
            }
        }

        // holds schedule lock
        private void doCancel() {
            if (this.tickScheduleHolder != null) {
                if (this.ownedBy != null) {
                    this.ownedBy.tickQueue.remove(this.tickScheduleHolder);
                }
                this.tickScheduleHolder = null;
            }
            if (this.taskScheduleHolder != null) {
                if (this.ownedBy != null) {
                    this.ownedBy.taskQueue.remove(this.taskScheduleHolder);
                }
                this.taskScheduleHolder = null;
            }
            this.state = STATE_CANCELLED;

            this.scheduledTo.tickCancelled(this);
        }

        private void finishTickExecution(final boolean cancel) {
            synchronized (this) {
                if (this.state != STATE_RUNNING && this.state != STATE_RUNNING_CANCELLED) {
                    throw new IllegalStateException("Must be running here");
                }

                if (cancel || this.state == STATE_RUNNING_CANCELLED) {
                    this.doCancel();
                    return;
                } else {
                    // be fair to other tasks, reset the task holder
                    // we expect the caller to invoke notifyTasks if needed
                    // (this also simplifies the logic here to determine the new state)
                    if (this.taskScheduleHolder != null) {
                        if (this.ownedBy != null) {
                            this.ownedBy.taskQueue.remove(this.taskScheduleHolder);
                        }
                        this.taskScheduleHolder = null;
                    }

                    // old tick holder is either null or taken, we must re-schedule
                    this.tickScheduleHolder = null;
                    this.reschedule(false);
                    this.state = STATE_IDLE;
                }
            }
        }
    }

    private static final class NodeThreads {

        private final TickThreadRunner[] threads;
        private final int nodeNumber;

        private NodeThreads(final TickThreadRunner[] threads, final int nodeNumber) {
            this.threads = threads;
            this.nodeNumber = nodeNumber;
        }
    }

    private static final class TickScheduleHolder {
        private static final AtomicLong ID_GENERATOR = new AtomicLong();
        private final long id = ID_GENERATOR.getAndIncrement();

        private static final Comparator<TickScheduleHolder> COMPARATOR = (final TickScheduleHolder h1, final TickScheduleHolder h2) -> {
            final int timeCompare = TimeUtil.compareTimes(h1.atNS, h2.atNS);
            if (timeCompare != 0) {
                return timeCompare;
            }

            return Long.compare(h1.id, h2.id);
        };
        private final ScheduledState tick;
        private final long atNS;
        private final boolean stolen;

        private TickScheduleHolder(final ScheduledState tick, final long atNS, final boolean stolen) {
            this.tick = tick;
            this.atNS = atNS;
            this.stolen = stolen;
        }
    }

    private static final class TaskScheduleHolder {
        private static final AtomicLong ID_GENERATOR = new AtomicLong();
        private final long id = ID_GENERATOR.getAndIncrement();

        private static final Comparator<TaskScheduleHolder> COMPARATOR = (final TaskScheduleHolder h1, final TaskScheduleHolder h2) -> {
            // want older timestamps first
            final int timeCompare = TimeUtil.compareTimes(h2.lastDrainedTasks, h1.lastDrainedTasks);
            if (timeCompare != 0) {
                return timeCompare;
            }

            return Long.compare(h1.id, h2.id);
        };
        private final ScheduledState task;
        private final long lastDrainedTasks;

        private TaskScheduleHolder(final ScheduledState task, final long lastDrainedTasks) {
            this.task = task;
            this.lastDrainedTasks = lastDrainedTasks;
        }
    }

    private static final class TickThreadRunner implements Runnable {

        private static final AtomicLong ID_GENERATOR = new AtomicLong();
        private final long id = ID_GENERATOR.getAndIncrement();

        // we force idle threads to wake up periodically so that they can steal tasks
        private static final long MAX_IDLE_TIME = TimeUnit.MILLISECONDS.toNanos(10L);

        private final Thread thread;
        private final StealingScheduledThreadPool scheduler;
        private final NodeThreads node;
        private final NodeThreads[] nodes;

        private static final int STATE_NOT_STARTED     = 1 << 0;
        private static final int STATE_IDLE            = 1 << 1;
        private static final int STATE_INTERRUPTED     = 1 << 2;
        private static final int STATE_EXECUTING_TASKS = 1 << 3;
        private static final int STATE_EXECUTING_TICK  = 1 << 4;
        private static final int STATE_HALTED          = 1 << 5;
        private volatile int state = STATE_NOT_STARTED;
        private static final VarHandle STATE_HANDLE = ConcurrentUtil.getVarHandle(TickThreadRunner.class, "state", int.class);

        private final ConcurrentSkipListMap<TickScheduleHolder, Boolean> tickQueue = new ConcurrentSkipListMap<>(TickScheduleHolder.COMPARATOR);
        private final ConcurrentSkipListMap<TaskScheduleHolder, Boolean> taskQueue = new ConcurrentSkipListMap<>(TaskScheduleHolder.COMPARATOR);

        private int getStateVolatile() {
            return (int)STATE_HANDLE.getVolatile(this);
        }

        private void setStateVolatile(final int value) {
            STATE_HANDLE.setVolatile(this, value);
        }

        private int exchangeStateVolatile(final int value) {
            return (int)STATE_HANDLE.getAndSet(this, value);
        }

        private int compareAndExchangeStateVolatile(final int expect, final int update) {
            return (int)STATE_HANDLE.compareAndExchange(this, expect, update);
        }

        private TickThreadRunner(final Thread thread, final StealingScheduledThreadPool scheduler, final NodeThreads node,
                                 final NodeThreads[] nodes) {
            this.thread = thread;
            this.scheduler = scheduler;
            this.node = node;
            this.nodes = nodes;
        }

        private void interruptIfIdle() {
            final int state = this.getStateVolatile();
            if (state == STATE_IDLE) {
                if (state == this.compareAndExchangeStateVolatile(state, STATE_INTERRUPTED)) {
                    LockSupport.unpark(this.thread);
                } // else: not idle
            } // else: not idle
        }

        private void halt() {
            if (STATE_IDLE == this.exchangeStateVolatile(STATE_HALTED)) {
                LockSupport.unpark(this.thread);
            }
        }

        private void tryInterrupt() {
            for (int state = this.getStateVolatile(), failures = 0;;) {
                for (int i = 0; i < failures; ++i) {
                    ConcurrentUtil.backoff();
                }

                ++failures;

                switch (state) {
                    case STATE_IDLE: {
                        if (state == (state = this.compareAndExchangeStateVolatile(state, STATE_INTERRUPTED))) {
                            // need to unpark if idle
                            LockSupport.unpark(this.thread);
                            return;
                        }
                        break;
                    }
                    case STATE_EXECUTING_TASKS: {
                        if (state == (state = this.compareAndExchangeStateVolatile(state, STATE_INTERRUPTED))) {
                            return;
                        }
                        break;
                    }

                    case STATE_NOT_STARTED:
                    case STATE_INTERRUPTED:
                    case STATE_EXECUTING_TICK:
                    case STATE_HALTED: {
                        return;
                    }
                    default: {
                        throw new IllegalStateException("Unknown state: " + state);
                    }
                }
            }
        }

        private TickScheduleHolder findFirstTick() {
            final Map.Entry<TickScheduleHolder, Boolean> first = this.tickQueue.firstEntry();

            return first == null ? null : first.getKey();
        }

        private TaskScheduleHolder findFirstTask() {
            final Map.Entry<TaskScheduleHolder, Boolean> first = this.taskQueue.firstEntry();

            return first == null ? null : first.getKey();
        }

        private void tryStealTask() {
            final TickScheduleHolder ourDeadlineHolder = this.findFirstTick();
            final long ourEarliest = ourDeadlineHolder == null ? TimeUtil.DEADLINE_NOT_SET : ourDeadlineHolder.atNS;

            final long now = System.nanoTime();
            final long stealThreshold = this.scheduler.stealThresholdNS;

            // find any task behind by the steal delay
            ScheduledState selected = null;
            // we adjust by the numa distance so that we can try to avoid shuffling tasks around nodes unless needed
            long selectedBehindAdjusted = Long.MIN_VALUE;
            int selectedDistance = Integer.MAX_VALUE;
            TickThreadRunner selectedRunner = null;

            for (final NodeThreads node : this.nodes) {
                final int rawDistance = this.scheduler.numa.getNumaDistance(this.node.nodeNumber, node.nodeNumber);
                final int distance = rawDistance <= 0 ? Integer.MAX_VALUE : rawDistance;

                for (final TickThreadRunner runner : node.threads) {
                    if (runner == this) {
                        // can't steal from ourselves
                        continue;
                    }

                    final TickScheduleHolder holder = runner.findFirstTick();

                    if (holder == null) {
                        // nothing to steal
                        continue;
                    }
                    if (holder.stolen) {
                        // already stolen
                        continue;
                    }

                    final long holderStart = holder.atNS;
                    if (ourEarliest != TimeUtil.DEADLINE_NOT_SET && TimeUtil.compareTimes(holderStart, ourEarliest) >= 0L) {
                        // this task is later than ours
                        continue;
                    }

                    final long behindBy = now - holderStart;
                    if (behindBy < stealThreshold) {
                        // below the steal threshold
                        continue;
                    }

                    // adjust the behind so that we prefer to steal tasks on closer nodes
                    // (tasks on closer nodes appear to be behind by more than tasks on farther nodes)
                    final long behindAdjusted = behindBy / distance;

                    if (behindAdjusted > selectedBehindAdjusted || (behindAdjusted == selectedBehindAdjusted && distance < selectedDistance)) {
                        selected = holder.tick;
                        selectedBehindAdjusted = behindAdjusted;
                        selectedDistance = distance;
                        selectedRunner = runner;
                    }
                }
            }

            if (selected != null) {
                // try to steal
                if (selected.trySteal(selectedRunner, this)) {
                    this.tryInterrupt();
                }
            }
        }

        private void begin() {
            // set numa
            if (this.scheduler.numa.isAvailable()) {
                this.scheduler.numa.setCurrentNumaAffinity(new int[]{ this.node.nodeNumber });
            }

            // use CAS in case we are halted already
            this.compareAndExchangeStateVolatile(STATE_NOT_STARTED, STATE_INTERRUPTED);
        }

        private void doRun() {
            while (this.mainLoop());
        }

        // returns true if deadline was reached, false if interrupted or halted
        private boolean executeTasksUntil(final long deadline) {
            final long timeSlice = this.scheduler.taskTimeSliceNS;
            for (;;) {
                // stealing a task here will set state to interrupted
                this.tryStealTask();

                // expect state idle here
                final TaskScheduleHolder taskHolder = this.findFirstTask();

                if (taskHolder == null) {
                    // no tasks, go to idle
                    while (this.getStateVolatile() == STATE_IDLE) {
                        Thread.interrupted();
                        final long sleep = deadline - System.nanoTime();
                        if (sleep <= 0L) {
                            return true;
                        }
                        LockSupport.parkNanos("Awaiting deadline", Math.min(sleep, MAX_IDLE_TIME));
                    }
                    // interrupted or halted
                    return false;
                } else {
                    // check deadline
                    if (deadline - System.nanoTime() <= 0L) {
                        return true;
                    }
                }

                if (STATE_IDLE != this.compareAndExchangeStateVolatile(STATE_IDLE, STATE_EXECUTING_TASKS)) {
                    // interrupted or halted
                    return false;
                }

                if (null == this.taskQueue.remove(taskHolder)) {
                    // need to move back to IDLE
                    this.compareAndExchangeStateVolatile(STATE_EXECUTING_TASKS, STATE_IDLE);
                    continue;
                }

                final ScheduledState task = taskHolder.task;

                if (task.trySetRunning(this)) {
                    // we can run
                    boolean cancel = false;
                    try {
                        final long start = System.nanoTime();
                        final long toEndAt = TimeUtil.getLeastTime(deadline, start + timeSlice);
                        final BooleanSupplier canContinueTasks = () -> {
                            if (TickThreadRunner.this.getStateVolatile() != STATE_EXECUTING_TASKS) {
                                // interrupted or halted
                                return false;
                            }

                            if (System.nanoTime() - toEndAt >= 0L) {
                                // past deadline
                                return false;
                            }

                            return true;
                        };

                        cancel = !task.tick.runTasks(canContinueTasks);
                    } finally {
                        task.finishTaskExecution(cancel);
                    }
                } // else: we cannot run, but we do need to possibly re-schedule since we took the task

                // this handles either case: we ran tasks or didn't
                // additionally, this handles cases where the task's owner is changed
                if (task.tick.hasTasks()) {
                    this.scheduler.notifyTasks(task.tick);
                }

                // move back to idle if nothing interrupted us
                this.compareAndExchangeStateVolatile(STATE_EXECUTING_TASKS, STATE_IDLE);
            }
        }

        private void tick(final TickScheduleHolder tickHolder) {
            if (!this.executeTasksUntil(tickHolder.atNS)) {
                // interrupted
                return;
            }

            if (STATE_IDLE != this.compareAndExchangeStateVolatile(STATE_IDLE, STATE_EXECUTING_TICK)) {
                // interrupted or halted
                return;
            }

            if (null == this.tickQueue.remove(tickHolder)) {
                // need to move back to IDLE
                this.compareAndExchangeStateVolatile(STATE_EXECUTING_TICK, STATE_IDLE);
                return;
            }

            final ScheduledState tick = tickHolder.tick;

            if (tick.trySetRunning(this)) {
                // we can run
                boolean cancel = false;
                try {
                    cancel = !tick.tick.runTick();
                } finally {
                    tick.finishTickExecution(cancel);
                }
            } // else: we cannot run, we do not need to re-schedule as this is handled by the task stealing logic

            // note: this handles the resetting of the task holder after ticking
            // additionally, this will handle task stealing as well
            if (tick.tick.hasTasks()) {
                this.scheduler.notifyTasks(tick.tick);
            }

            // move to interrupted to process more tasks
            this.compareAndExchangeStateVolatile(STATE_EXECUTING_TICK, STATE_INTERRUPTED);
        }

        private boolean mainLoop() {
            // block until tasks are scheduled
            while (this.getStateVolatile() == STATE_IDLE) {
                this.tryStealTask();

                Thread.interrupted();
                LockSupport.parkNanos("Idle parking", MAX_IDLE_TIME);
            }

            // note: either interrupted or halted here

            // note: we need to actually transition to another state before reading
            //       from the tick queue, as additions to the tick queue that change the head will interrupt
            if (STATE_INTERRUPTED != this.compareAndExchangeStateVolatile(STATE_INTERRUPTED, STATE_IDLE)) {
                final int curr = this.getStateVolatile();
                if (STATE_HALTED != curr) {
                    throw new IllegalStateException("Unknown state: " + curr);
                }
                // halted
                return false;
            }

            final TickScheduleHolder toTick = this.findFirstTick();

            // note: state IDLE here

            if (toTick == null) {
                // we should also have no scheduled tasks

                // attempt to steal tasks
                this.tryStealTask();
                return true;
            } else {
                // attempt to wait until tick deadline and tick the task
                this.tick(toTick);
            }

            return true;
        }

        private void die() {
            this.scheduler.die(this);
        }

        @Override
        public void run() {
            try {
                this.begin();
                this.doRun();
            } finally {
                this.die();
            }
        }
    }
}
