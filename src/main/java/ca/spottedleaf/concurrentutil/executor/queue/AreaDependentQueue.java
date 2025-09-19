package ca.spottedleaf.concurrentutil.executor.queue;

import ca.spottedleaf.concurrentutil.executor.PrioritisedExecutor;
import ca.spottedleaf.concurrentutil.lock.ReentrantAreaLock;
import ca.spottedleaf.concurrentutil.map.ConcurrentLong2ReferenceChainedHashTable;
import ca.spottedleaf.concurrentutil.util.IntPairUtil;
import ca.spottedleaf.concurrentutil.util.Priority;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicLong;

public final class AreaDependentQueue {

    /**
     * Whether to order tasks by (lower) stream id after (higher) priority
     */
    public static final long FLAG_ORDER_BY_STREAM = 1L << 0;

    // need an id generator to break ties in the task queues to prevent deadlock
    // if two overlapping entries tie, then it is possible for the order in the
    // overlapped queues to be different and prevent forward progress
    private final AtomicLong idGenerator = new AtomicLong();

    private final PrioritisedExecutor executor;
    private final ConcurrentLong2ReferenceChainedHashTable<Position> tasks = ConcurrentLong2ReferenceChainedHashTable.createWithCapacity(1024, 0.25f);
    // lock order: acquire this lock ("area lock") before task lock ("monitor")
    private final ReentrantAreaLock lock;

    private final boolean orderByLowerStream;

    public AreaDependentQueue(final PrioritisedExecutor executor, final int lockShift) {
        this(executor, lockShift, 0L);
    }

    public AreaDependentQueue(final PrioritisedExecutor executor, final int lockShift,
                              final long flags) {
        this.executor = executor;
        this.lock = new ReentrantAreaLock(lockShift);
        this.orderByLowerStream = (flags & FLAG_ORDER_BY_STREAM) != 0L;
    }

    public long generateNextSubOrder() {
        return this.executor.generateNextSubOrder();
    }

    public PrioritisedExecutor.PrioritisedTask createTask(final int x, final int y, final int radius,
                                                          final Runnable run) {
        return this.createTask(x - radius, y - radius, x + radius, y + radius, run);
    }

    public PrioritisedExecutor.PrioritisedTask createTask(final int x, final int y, final int radius,
                                                          final Runnable run, final Priority priority) {
        return this.createTask(x - radius, y - radius, x + radius, y + radius, run, priority);
    }

    public PrioritisedExecutor.PrioritisedTask createTask(final int x, final int y, final int radius,
                                                          final Runnable run, final Priority priority,
                                                          final long subOrder, final long stream) {
        return this.createTask(x - radius, y - radius, x + radius, y + radius, run, priority, subOrder, stream);
    }

    public PrioritisedExecutor.PrioritisedTask createTask(final int minX, final int minY, final int maxX, final int maxY,
                                                          final Runnable run) {
        return this.createTask(minX, minY, maxX, maxY, run, Priority.NORMAL);
    }

    public PrioritisedExecutor.PrioritisedTask createTask(final int minX, final int minY, final int maxX, final int maxY,
                                                          final Runnable run, final Priority priority) {
        return this.createTask(minX, minY, maxX, maxY, run, priority, this.generateNextSubOrder(), 0L);
    }

    public PrioritisedExecutor.PrioritisedTask createTask(final int minX, final int minY, final int maxX, final int maxY,
                                                          final Runnable run, final Priority priority,
                                                          final long subOrder, final long stream) {
        if (minX > maxX || minY > maxY) {
            throw new IllegalArgumentException("min should be <= max");
        }

        return new PositionedTask(
                this, this.idGenerator.getAndIncrement(), minX, minY, maxX, maxY,
                run, priority, subOrder, stream
        );
    }


    public PrioritisedExecutor.PrioritisedTask queueTask(final int x, final int y, final int radius,
                                                         final Runnable run) {
        final PrioritisedExecutor.PrioritisedTask ret = this.createTask(x, y, radius, run);
        ret.queue();
        return ret;
    }

    public PrioritisedExecutor.PrioritisedTask queueTask(final int x, final int y, final int radius,
                                                         final Runnable run, final Priority priority) {
        final PrioritisedExecutor.PrioritisedTask ret = this.createTask(x, y, radius, run, priority);
        ret.queue();
        return ret;
    }

    public PrioritisedExecutor.PrioritisedTask queueTask(final int x, final int y, final int radius,
                                                         final Runnable run, final Priority priority,
                                                         final long subOrder, final long stream) {
        final PrioritisedExecutor.PrioritisedTask ret = this.createTask(x, y, radius, run, priority, subOrder, stream);
        ret.queue();
        return ret;
    }

    public PrioritisedExecutor.PrioritisedTask queueTask(final int minX, final int minY, final int maxX, final int maxY,
                                                         final Runnable run) {
        final PrioritisedExecutor.PrioritisedTask ret = this.createTask(minX, minY, maxX, maxY, run);
        ret.queue();
        return ret;
    }

    public PrioritisedExecutor.PrioritisedTask queueTask(final int minX, final int minY, final int maxX, final int maxY,
                                                         final Runnable run, final Priority priority) {
        final PrioritisedExecutor.PrioritisedTask ret = this.createTask(minX, minY, maxX, maxY, run, priority);
        ret.queue();
        return ret;
    }

    public PrioritisedExecutor.PrioritisedTask queueTask(final int minX, final int minY, final int maxX, final int maxY,
                                                         final Runnable run, final Priority priority,
                                                         final long subOrder, final long stream) {
        final PrioritisedExecutor.PrioritisedTask ret = this.createTask(minX, minY, maxX, maxY, run, priority, subOrder, stream);
        ret.queue();
        return ret;
    }

    // returns true if the task is first in queue and needs to be scheduled
    boolean add(final PositionedTask positionedTask, final QueuedTask task,
                final List<PrioritisedExecutor.PrioritisedTask> toCancel) {
        final int minX = positionedTask.minX;
        final int minY = positionedTask.minY;
        final int maxX = positionedTask.maxX;
        final int maxY = positionedTask.maxY;

        boolean first = true;

        for (int currY = minY; currY <= maxY; ++currY) {
            for (int currX = minX; currX <= maxX; ++currX) {
                final long key = IntPairUtil.key(currX, currY);
                final Position position = this.tasks.computeIfAbsent(key, (final long keyInMap) -> {
                    return new Position();
                });

                first &= position.addTask(task, toCancel);
            }
        }

        return first;
    }

    // must hold lock over bounds of task
    void remove(final PositionedTask positionedTask, final QueuedTask task,
                final List<PrioritisedExecutor.PrioritisedTask> toQueue) {
        final int minX = positionedTask.minX;
        final int minY = positionedTask.minY;
        final int maxX = positionedTask.maxX;
        final int maxY = positionedTask.maxY;

        for (int currY = minY; currY <= maxY; ++currY) {
            for (int currX = minX; currX <= maxX; ++currX) {
                final long key = IntPairUtil.key(currX, currY);
                final Position position = this.tasks.get(key);

                if (!position.removeTask(task, toQueue)) {
                    this.tasks.remove(key);
                }
            }
        }
    }


    // must hold lock over bounds of task
    void adjust(final PositionedTask positionedTask, final QueuedTask oldTask, final QueuedTask newTask,
                final List<PrioritisedExecutor.PrioritisedTask> toQueue,
                final List<PrioritisedExecutor.PrioritisedTask> toCancel) {
        final int minX = positionedTask.minX;
        final int minY = positionedTask.minY;
        final int maxX = positionedTask.maxX;
        final int maxY = positionedTask.maxY;

        for (int currY = minY; currY <= maxY; ++currY) {
            for (int currX = minX; currX <= maxX; ++currX) {
                this.tasks.get(IntPairUtil.key(currX, currY)).adjustTask(
                        oldTask, newTask, toQueue, toCancel
                );
            }
        }
    }

    private static final class Position {

        private final PriorityQueue<QueuedTask> queue = new PriorityQueue<>();
        private QueuedTask firstOrRunning;

        // assume that lock is held for all positions of task
        // it is the callers responsibility to schedule the task if the waiting pos == 0
        // returns true if the queued task is the first in queue
        boolean addTask(final QueuedTask task, final List<PrioritisedExecutor.PrioritisedTask> toCancel) {
            this.queue.add(task);
            if (this.queue.peek() != task) {
                // behind another task
                task.positionedTask.incWaitingPosRaw();
                return false;
            }

            final QueuedTask currentFirst = this.firstOrRunning;

            if (currentFirst == null || currentFirst.positionedTask.incWaitingPos(currentFirst, toCancel)) {
                this.firstOrRunning = task;
                return true;
            }
            // else: != null && task is executing, so we need to increment the parameter's waiting pos count

            task.positionedTask.incWaitingPosRaw();
            return false;
        }

        // assume that lock is held for all positions of task
        // assume that the task will not be re-queued (cancelled or executed)
        // returns true if this queue is not empty after removing the task
        boolean removeTask(final QueuedTask task, final List<PrioritisedExecutor.PrioritisedTask> toQueue) {
            this.queue.remove(task);

            final QueuedTask currentFirst = this.firstOrRunning;
            // note: currentFirst != null

            if (task != currentFirst) {
                // no changes needed, something is already first - we did not remove it
                return true;
            }

            final QueuedTask nextFirst = this.queue.peek();

            if (nextFirst == null) {
                // simple case: no task to move to first
                this.firstOrRunning = null;
                return false;
            }

            this.firstOrRunning = nextFirst;
            nextFirst.positionedTask.decWaitingPos(nextFirst, toQueue);

            return true;
        }

        // assume that lock is held for all positions of task
        // it is the callers responsibility to schedule the task if the waiting pos == 0
        // assume that the task state is STATE_WAITING or STATE_SCHEDULED
        // assume that the caller will add the old task to toCancel
        void adjustTask(final QueuedTask oldTask, final QueuedTask newTask,
                        final List<PrioritisedExecutor.PrioritisedTask> toQueue,
                        final List<PrioritisedExecutor.PrioritisedTask> toCancel) {
            this.queue.remove(oldTask);
            this.queue.add(newTask);

            final QueuedTask currentFirst = this.firstOrRunning;
            final QueuedTask nextFirst = this.queue.peek();

            if (currentFirst == nextFirst) {
                // nothing to do
                return;
            }

            if (currentFirst == oldTask) {
                this.firstOrRunning = nextFirst;

                if (nextFirst != newTask) {
                    nextFirst.positionedTask.decWaitingPos(nextFirst, toQueue);
                    if (!oldTask.positionedTask.incWaitingPos(oldTask, null)) {
                        throw new IllegalStateException("Old task may not be in executing state");
                    }
                } // else: do not adjust waiting count (currentFirst == oldTask && nextFirst == newTask -> same queued task)
                return;
            }

            if (!currentFirst.positionedTask.incWaitingPos(currentFirst, toCancel)) {
                // failed to adjust waiting pos, so we cannot adjust first
                return;
            }

            this.firstOrRunning = nextFirst;

            if (nextFirst == newTask) {
                nextFirst.positionedTask.decWaitingPosRaw();
            } else {
                nextFirst.positionedTask.decWaitingPos(newTask, toQueue);
            }
        }
    }

    private static final class PositionedTask implements PrioritisedExecutor.PrioritisedTask {

        // queue() not invoked yet
        // waiting pos count = 0
        // scheduledTask = null
        private static final int STATE_NOT_QUEUED = 1 << 0;

        // waiting pos count >= 0
        // scheduledTask != null and scheduledTask.queue() not invoked
        private static final int STATE_WAITING = 1 << 1;

        // waiting pos count = 0
        // scheduledTask != null and scheduledTask.queue() invoked
        private static final int STATE_SCHEDULED = 1 << 2;

        // waiting pos count = 0
        // scheduledTask != null
        private static final int STATE_EXECUTING = 1 << 3;

        private int waitingPosCount;
        private int state = STATE_NOT_QUEUED;
        private QueuedTask scheduledTask;

        private final AreaDependentQueue areaDependentQueue;

        private final long taskId;

        private final int minX;
        private final int minY;
        private final int maxX;
        private final int maxY;
        private final int maxWaitingPos;

        private final Runnable run;

        private PositionedTask(final AreaDependentQueue areaDependentQueue, final long taskId,
                               final int minX, final int minY, final int maxX, final int maxY,
                               final Runnable run, final Priority priority, final long subOrder,
                               final long stream) {
            this.areaDependentQueue = areaDependentQueue;
            this.taskId = taskId;
            this.minX = minX;
            this.minY = minY;
            this.maxX = maxX;
            this.maxY = maxY;
            this.maxWaitingPos = (this.maxX - this.minX + 1) * (this.maxY - this.minY + 1);
            this.run = run;

            this.scheduledTask = new QueuedTask(
                    priority, subOrder, stream, this, areaDependentQueue.executor
            );
        }

        // unsynchronised. use only if state is known to be WAITING and that state cannot change
        void incWaitingPosRaw() {
            if (++this.waitingPosCount > this.maxWaitingPos) {
                throw new IllegalStateException("Waiting pos too large");
            }
        }

        // unsynchronised. use only if state is known to be WAITING and that state cannot change
        void decWaitingPosRaw() {
            if (--this.waitingPosCount < 0) {
                throw new IllegalStateException("Waiting pos too small");
            }
        }

        // false -> EXECUTING
        // true -> state remained at WAITING or state moved from SCHEDULED to WAITING
        synchronized boolean incWaitingPos(final QueuedTask task, final List<PrioritisedExecutor.PrioritisedTask> toCancel) {
            final int state = this.state;

            switch (state) {
                case STATE_WAITING: {
                    this.incWaitingPosRaw();
                    return true;
                }
                case STATE_SCHEDULED: {
                    this.state = STATE_WAITING;
                    this.incWaitingPosRaw();

                    if (toCancel != null) {
                        toCancel.add(task.swapTask(this.areaDependentQueue.executor));
                    }

                    return true;
                }
                case STATE_EXECUTING: {
                    return false;
                }
                default: {
                    throw new IllegalStateException("Illegal or unknown state: " + state);
                }
            }
        }

        synchronized void decWaitingPos(final QueuedTask task, final List<PrioritisedExecutor.PrioritisedTask> toQueue) {
            final int state = this.state;

            if (state == STATE_WAITING) {
                final int newWaiting = --this.waitingPosCount;

                if (newWaiting == 0) {
                    this.state = STATE_SCHEDULED;
                    toQueue.add(task.prioritisedTask);
                    return;
                }

                if (newWaiting < 0) {
                    throw new IllegalStateException("Waiting pos too small");
                }
                return;
            }

            throw new IllegalStateException("Illegal or unknown state: " + state);
        }

        synchronized boolean tryMarkExecuting(final QueuedTask scheduledTask) {
            if (this.scheduledTask != scheduledTask || this.state != STATE_SCHEDULED) {
                return false;
            }
            this.state = STATE_EXECUTING;
            return true;
        }

        void finishTask(final QueuedTask scheduledTask) {
            final List<PrioritisedExecutor.PrioritisedTask> toQueue = new ArrayList<>();

            final ReentrantAreaLock.Node lock = this.areaDependentQueue.lock.lock(
                    this.minX, this.minY, this.maxX, this.maxY
            );
            try {
                this.areaDependentQueue.remove(this, scheduledTask, toQueue);
            } finally {
                this.areaDependentQueue.lock.unlock(lock);
            }

            for (int i = 0, len = toQueue.size(); i < len; ++i) {
                toQueue.get(i).queue();
            }
        }

        // must hold area lock + monitor on this object
        private void adjustPriority(final Priority priority, final long subOrder, final long stream,
                                    final List<PrioritisedExecutor.PrioritisedTask> toQueue,
                                    final List<PrioritisedExecutor.PrioritisedTask> toCancel) {
            final QueuedTask oldTask = this.scheduledTask;
            final QueuedTask newTask = this.scheduledTask = new QueuedTask(
                    priority, subOrder, stream, this, this.areaDependentQueue.executor
            );

            // not done by adjust
            toCancel.add(oldTask.prioritisedTask);

            this.areaDependentQueue.adjust(this, oldTask, newTask, toQueue, toCancel);
            // check waiting pos count, as this is not done by adjust
            if (this.waitingPosCount == 0) {
                // note: state may already be SCHEDULED, but this is OK
                this.state = STATE_SCHEDULED;
                toQueue.add(newTask.prioritisedTask);
            }
        }

        @Override
        public PrioritisedExecutor getExecutor() {
            return this.areaDependentQueue.executor;
        }

        @Override
        public boolean queue() {
            if (this.state != STATE_NOT_QUEUED) {
                synchronized (this) {
                    // acquire lock to synchronise-with thread that queued or cancelled
                    return false;
                }
            }

            final List<PrioritisedExecutor.PrioritisedTask> toCancel = new ArrayList<>();
            final PrioritisedExecutor.PrioritisedTask toQueue;

            final ReentrantAreaLock.Node lock = this.areaDependentQueue.lock.lock(
                    this.minX, this.minY, this.maxX, this.maxY
            );
            try {
                // shift state to WAITING
                synchronized (this) {
                    if (this.state != STATE_NOT_QUEUED) {
                        // queued or cancelled
                        return false;
                    }

                    final boolean queue = this.areaDependentQueue.add(this, this.scheduledTask, toCancel);
                    if (queue) {
                        this.state = STATE_SCHEDULED;
                        toQueue = this.scheduledTask.prioritisedTask;
                    } else {
                        this.state = STATE_WAITING;
                        toQueue = null;
                    }
                }
            } finally {
                this.areaDependentQueue.lock.unlock(lock);
            }

            for (int i = 0, len = toCancel.size(); i < len; ++i) {
                toCancel.get(i).cancel();
            }

            if (toQueue != null) {
                toQueue.queue();
            }

            return true;
        }

        @Override
        public boolean isQueued() {
            synchronized (this) {
                return this.state != STATE_NOT_QUEUED && this.state != STATE_EXECUTING;
            }
        }

        @Override
        public boolean cancel() {
            if (this.state == STATE_NOT_QUEUED || this.state == STATE_EXECUTING) {
                // try to cancel without acquiring area lock OR synchronise with the thread that cancelled the task
                synchronized (this) {
                    if (this.state == STATE_NOT_QUEUED) {
                        this.state = STATE_EXECUTING;
                        return true;
                    } else if (this.state == STATE_EXECUTING) {
                        // cancelled or running
                        return false;
                    } // else: queued, fall through to area lock acquire
                }
            }

            final PrioritisedExecutor.PrioritisedTask toCancel;
            final List<PrioritisedExecutor.PrioritisedTask> toQueue = new ArrayList<>();

            final ReentrantAreaLock.Node lock = this.areaDependentQueue.lock.lock(
                    this.minX, this.minY, this.maxX, this.maxY
            );
            try {
                // required to acquire monitor to deal with STATE_SCHEDULED
                // we could get away with not using the monitor if the state is STATE_WAITING, as
                // we hold the area lock
                synchronized (this) {
                    // note: state != STATE_NOT_QUEUED
                    final int state = this.state;
                    switch (this.state) {
                        case STATE_WAITING: {
                            this.state = STATE_EXECUTING;
                            toCancel = null;
                            break;
                        }
                        case STATE_SCHEDULED: {
                            this.state = STATE_EXECUTING;
                            // not required, but no point in holding a scheduled slot for a cancelled task
                            toCancel = this.scheduledTask.prioritisedTask;
                            break;
                        }
                        case STATE_EXECUTING: {
                            return false;
                        }
                        default: {
                            throw new IllegalStateException("Unknown state: " + state);
                        }
                    }
                }

                this.areaDependentQueue.remove(this, this.scheduledTask, toQueue);
            } finally {
                this.areaDependentQueue.lock.unlock(lock);
            }

            if (toCancel != null) {
                toCancel.cancel();
            }

            for (int i = 0, len = toQueue.size(); i < len; ++i) {
                toQueue.get(i).queue();
            }

            return true;
        }

        @Override
        public boolean execute() {
            if (!this.cancel()) {
                return false;
            }

            this.run.run();
            return true;
        }

        @Override
        public Priority getPriority() {
            synchronized (this) {
                return this.state == STATE_EXECUTING ? Priority.COMPLETING : this.scheduledTask.priority;
            }
        }

        @Override
        public boolean setPriority(final Priority priority) {
            switch (this.state) {
                case STATE_NOT_QUEUED: {
                    // avoid acquiring area lock when we are not queued
                    synchronized (this) {
                        if (this.state != STATE_NOT_QUEUED) {
                            // something changed, fall through
                            break;
                        }

                        if (this.scheduledTask.priority == priority) {
                            return false;
                        }

                        // do not need to re-insert, we are not queued
                        this.scheduledTask.priority = priority;
                        return true;
                    }
                }
                case STATE_EXECUTING: {
                    synchronized (this) {
                        // acquire lock to synchronise-with thread that queued or cancelled
                        return false;
                    }
                }
                // default: fall through
            }

            final List<PrioritisedExecutor.PrioritisedTask> toQueue = new ArrayList<>();
            final List<PrioritisedExecutor.PrioritisedTask> toCancel = new ArrayList<>();

            final ReentrantAreaLock.Node lock = this.areaDependentQueue.lock.lock(
                    this.minX, this.minY, this.maxX, this.maxY
            );
            try {
                synchronized (this) {
                    if (this.state == STATE_EXECUTING) {
                        return false;
                    }

                    if (this.scheduledTask.priority == priority) {
                        return false;
                    }

                    this.adjustPriority(priority, this.scheduledTask.subOrder, this.scheduledTask.stream, toQueue, toCancel);
                }
            } finally {
                this.areaDependentQueue.lock.unlock(lock);
            }

            for (int i = 0, len = toCancel.size(); i < len; ++i) {
                toCancel.get(i).cancel();
            }
            for (int i = 0, len = toQueue.size(); i < len; ++i) {
                toQueue.get(i).queue();
            }

            return true;
        }

        @Override
        public boolean raisePriority(final Priority priority) {
            switch (this.state) {
                case STATE_NOT_QUEUED: {
                    // avoid acquiring area lock when we are not queued
                    synchronized (this) {
                        if (this.state != STATE_NOT_QUEUED) {
                            // something changed, fall through
                            break;
                        }

                        if (this.scheduledTask.priority.isHigherOrEqualPriority(priority)) {
                            return false;
                        }

                        // do not need to re-insert, we are not queued
                        this.scheduledTask.priority = priority;
                        return true;
                    }
                }
                case STATE_EXECUTING: {
                    synchronized (this) {
                        // acquire lock to synchronise-with thread that queued or cancelled
                        return false;
                    }
                }
                // default: fall through
            }

            final List<PrioritisedExecutor.PrioritisedTask> toQueue = new ArrayList<>();
            final List<PrioritisedExecutor.PrioritisedTask> toCancel = new ArrayList<>();

            final ReentrantAreaLock.Node lock = this.areaDependentQueue.lock.lock(
                    this.minX, this.minY, this.maxX, this.maxY
            );
            try {
                synchronized (this) {
                    if (this.state == STATE_EXECUTING) {
                        return false;
                    }

                    if (this.scheduledTask.priority.isHigherOrEqualPriority(priority)) {
                        return false;
                    }

                    this.adjustPriority(priority, this.scheduledTask.subOrder, this.scheduledTask.stream, toQueue, toCancel);
                }
            } finally {
                this.areaDependentQueue.lock.unlock(lock);
            }

            for (int i = 0, len = toCancel.size(); i < len; ++i) {
                toCancel.get(i).cancel();
            }
            for (int i = 0, len = toQueue.size(); i < len; ++i) {
                toQueue.get(i).queue();
            }

            return true;
        }

        @Override
        public boolean lowerPriority(final Priority priority) {
            switch (this.state) {
                case STATE_NOT_QUEUED: {
                    // avoid acquiring area lock when we are not queued
                    synchronized (this) {
                        if (this.state != STATE_NOT_QUEUED) {
                            // something changed, fall through
                            break;
                        }

                        if (this.scheduledTask.priority.isLowerOrEqualPriority(priority)) {
                            return false;
                        }

                        // do not need to re-insert, we are not queued
                        this.scheduledTask.priority = priority;
                        return true;
                    }
                }
                case STATE_EXECUTING: {
                    synchronized (this) {
                        // acquire lock to synchronise-with thread that queued or cancelled
                        return false;
                    }
                }
                // default: fall through
            }

            final List<PrioritisedExecutor.PrioritisedTask> toQueue = new ArrayList<>();
            final List<PrioritisedExecutor.PrioritisedTask> toCancel = new ArrayList<>();

            final ReentrantAreaLock.Node lock = this.areaDependentQueue.lock.lock(
                    this.minX, this.minY, this.maxX, this.maxY
            );
            try {
                synchronized (this) {
                    if (this.state == STATE_EXECUTING) {
                        return false;
                    }

                    if (this.scheduledTask.priority.isLowerOrEqualPriority(priority)) {
                        return false;
                    }

                    this.adjustPriority(priority, this.scheduledTask.subOrder, this.scheduledTask.stream, toQueue, toCancel);
                }
            } finally {
                this.areaDependentQueue.lock.unlock(lock);
            }

            for (int i = 0, len = toCancel.size(); i < len; ++i) {
                toCancel.get(i).cancel();
            }
            for (int i = 0, len = toQueue.size(); i < len; ++i) {
                toQueue.get(i).queue();
            }

            return true;
        }

        @Override
        public long getSubOrder() {
            synchronized (this) {
                return this.state == STATE_EXECUTING ? 0L : this.scheduledTask.subOrder;
            }
        }

        @Override
        public boolean setSubOrder(final long subOrder) {
            switch (this.state) {
                case STATE_NOT_QUEUED: {
                    // avoid acquiring area lock when we are not queued
                    synchronized (this) {
                        if (this.state != STATE_NOT_QUEUED) {
                            // something changed, fall through
                            break;
                        }

                        if (this.scheduledTask.subOrder == subOrder) {
                            return false;
                        }

                        // do not need to re-insert, we are not queued
                        this.scheduledTask.subOrder = subOrder;
                        return true;
                    }
                }
                case STATE_EXECUTING: {
                    synchronized (this) {
                        // acquire lock to synchronise-with thread that queued or cancelled
                        return false;
                    }
                }
                // default: fall through
            }

            final List<PrioritisedExecutor.PrioritisedTask> toQueue = new ArrayList<>();
            final List<PrioritisedExecutor.PrioritisedTask> toCancel = new ArrayList<>();

            final ReentrantAreaLock.Node lock = this.areaDependentQueue.lock.lock(
                    this.minX, this.minY, this.maxX, this.maxY
            );
            try {
                synchronized (this) {
                    if (this.state == STATE_EXECUTING) {
                        return false;
                    }

                    if (this.scheduledTask.subOrder == subOrder) {
                        return false;
                    }

                    this.adjustPriority(this.scheduledTask.priority, subOrder, this.scheduledTask.stream, toQueue, toCancel);
                }
            } finally {
                this.areaDependentQueue.lock.unlock(lock);
            }

            for (int i = 0, len = toCancel.size(); i < len; ++i) {
                toCancel.get(i).cancel();
            }
            for (int i = 0, len = toQueue.size(); i < len; ++i) {
                toQueue.get(i).queue();
            }

            return true;
        }

        @Override
        public boolean raiseSubOrder(final long subOrder) {
            switch (this.state) {
                case STATE_NOT_QUEUED: {
                    // avoid acquiring area lock when we are not queued
                    synchronized (this) {
                        if (this.state != STATE_NOT_QUEUED) {
                            // something changed, fall through
                            break;
                        }

                        if (this.scheduledTask.subOrder >= subOrder) {
                            return false;
                        }

                        // do not need to re-insert, we are not queued
                        this.scheduledTask.subOrder = subOrder;
                        return true;
                    }
                }
                case STATE_EXECUTING: {
                    synchronized (this) {
                        // acquire lock to synchronise-with thread that queued or cancelled
                        return false;
                    }
                }
                // default: fall through
            }

            final List<PrioritisedExecutor.PrioritisedTask> toQueue = new ArrayList<>();
            final List<PrioritisedExecutor.PrioritisedTask> toCancel = new ArrayList<>();

            final ReentrantAreaLock.Node lock = this.areaDependentQueue.lock.lock(
                    this.minX, this.minY, this.maxX, this.maxY
            );
            try {
                synchronized (this) {
                    if (this.state == STATE_EXECUTING) {
                        return false;
                    }

                    if (this.scheduledTask.subOrder >= subOrder) {
                        return false;
                    }

                    this.adjustPriority(this.scheduledTask.priority, subOrder, this.scheduledTask.stream, toQueue, toCancel);
                }
            } finally {
                this.areaDependentQueue.lock.unlock(lock);
            }

            for (int i = 0, len = toCancel.size(); i < len; ++i) {
                toCancel.get(i).cancel();
            }
            for (int i = 0, len = toQueue.size(); i < len; ++i) {
                toQueue.get(i).queue();
            }

            return true;
        }

        @Override
        public boolean lowerSubOrder(final long subOrder) {
            switch (this.state) {
                case STATE_NOT_QUEUED: {
                    // avoid acquiring area lock when we are not queued
                    synchronized (this) {
                        if (this.state != STATE_NOT_QUEUED) {
                            // something changed, fall through
                            break;
                        }

                        if (this.scheduledTask.subOrder <= subOrder) {
                            return false;
                        }

                        // do not need to re-insert, we are not queued
                        this.scheduledTask.subOrder = subOrder;
                        return true;
                    }
                }
                case STATE_EXECUTING: {
                    synchronized (this) {
                        // acquire lock to synchronise-with thread that queued or cancelled
                        return false;
                    }
                }
                // default: fall through
            }

            final List<PrioritisedExecutor.PrioritisedTask> toQueue = new ArrayList<>();
            final List<PrioritisedExecutor.PrioritisedTask> toCancel = new ArrayList<>();

            final ReentrantAreaLock.Node lock = this.areaDependentQueue.lock.lock(
                    this.minX, this.minY, this.maxX, this.maxY
            );
            try {
                synchronized (this) {
                    if (this.state == STATE_EXECUTING) {
                        return false;
                    }

                    if (this.scheduledTask.subOrder <= subOrder) {
                        return false;
                    }

                    this.adjustPriority(this.scheduledTask.priority, subOrder, this.scheduledTask.stream, toQueue, toCancel);
                }
            } finally {
                this.areaDependentQueue.lock.unlock(lock);
            }

            for (int i = 0, len = toCancel.size(); i < len; ++i) {
                toCancel.get(i).cancel();
            }
            for (int i = 0, len = toQueue.size(); i < len; ++i) {
                toQueue.get(i).queue();
            }

            return true;
        }

        @Override
        public long getStream() {
            synchronized (this) {
                return this.state == STATE_EXECUTING ? 0L : this.scheduledTask.stream;
            }
        }

        @Override
        public boolean setStream(final long stream) {
            switch (this.state) {
                case STATE_NOT_QUEUED: {
                    // avoid acquiring area lock when we are not queued
                    synchronized (this) {
                        if (this.state != STATE_NOT_QUEUED) {
                            // something changed, fall through
                            break;
                        }

                        if (this.scheduledTask.stream == stream) {
                            return false;
                        }

                        // do not need to re-insert, we are not queued
                        this.scheduledTask.stream = stream;
                        return true;
                    }
                }
                case STATE_EXECUTING: {
                    synchronized (this) {
                        // acquire lock to synchronise-with thread that queued or cancelled
                        return false;
                    }
                }
                // default: fall through
            }

            final List<PrioritisedExecutor.PrioritisedTask> toQueue = new ArrayList<>();
            final List<PrioritisedExecutor.PrioritisedTask> toCancel = new ArrayList<>();

            final ReentrantAreaLock.Node lock = this.areaDependentQueue.lock.lock(
                    this.minX, this.minY, this.maxX, this.maxY
            );
            try {
                synchronized (this) {
                    if (this.state == STATE_EXECUTING) {
                        return false;
                    }

                    if (this.scheduledTask.stream == stream) {
                        return false;
                    }

                    this.adjustPriority(this.scheduledTask.priority, this.scheduledTask.subOrder, stream, toQueue, toCancel);
                }
            } finally {
                this.areaDependentQueue.lock.unlock(lock);
            }

            for (int i = 0, len = toCancel.size(); i < len; ++i) {
                toCancel.get(i).cancel();
            }
            for (int i = 0, len = toQueue.size(); i < len; ++i) {
                toQueue.get(i).queue();
            }

            return true;
        }

        @Override
        public boolean setPrioritySubOrderStream(final Priority priority, final long subOrder, final long stream) {
            switch (this.state) {
                case STATE_NOT_QUEUED: {
                    // avoid acquiring area lock when we are not queued
                    synchronized (this) {
                        if (this.state != STATE_NOT_QUEUED) {
                            // something changed, fall through
                            break;
                        }

                        if (this.scheduledTask.priority == priority && this.scheduledTask.subOrder == subOrder
                                && this.scheduledTask.stream == stream) {
                            return false;
                        }

                        // do not need to re-insert, we are not queued
                        this.scheduledTask.priority = priority;
                        this.scheduledTask.subOrder = subOrder;
                        this.scheduledTask.stream = stream;
                        return true;
                    }
                }
                case STATE_EXECUTING: {
                    synchronized (this) {
                        // acquire lock to synchronise-with thread that queued or cancelled
                        return false;
                    }
                }
                // default: fall through
            }

            final List<PrioritisedExecutor.PrioritisedTask> toQueue = new ArrayList<>();
            final List<PrioritisedExecutor.PrioritisedTask> toCancel = new ArrayList<>();

            final ReentrantAreaLock.Node lock = this.areaDependentQueue.lock.lock(
                    this.minX, this.minY, this.maxX, this.maxY
            );
            try {
                synchronized (this) {
                    if (this.state == STATE_EXECUTING) {
                        return false;
                    }

                    if (this.scheduledTask.priority == priority && this.scheduledTask.subOrder == subOrder
                            && this.scheduledTask.stream == stream) {
                        return false;
                    }

                    this.adjustPriority(priority, subOrder, stream, toQueue, toCancel);
                }
            } finally {
                this.areaDependentQueue.lock.unlock(lock);
            }

            for (int i = 0, len = toCancel.size(); i < len; ++i) {
                toCancel.get(i).cancel();
            }
            for (int i = 0, len = toQueue.size(); i < len; ++i) {
                toQueue.get(i).queue();
            }

            return true;
        }

        @Override
        public PrioritisedExecutor.PriorityState getPriorityState() {
            synchronized (this) {
                if (this.state == STATE_EXECUTING) {
                    return null;
                }

                return new PrioritisedExecutor.PriorityState(
                        this.scheduledTask.priority, this.scheduledTask.subOrder, this.scheduledTask.stream
                );
            }
        }
    }

    private static final class QueuedTask implements Runnable, Comparable<QueuedTask> {

        private Priority priority;
        private long subOrder;
        private long stream;

        private final PositionedTask positionedTask;
        private PrioritisedExecutor.PrioritisedTask prioritisedTask;

        private QueuedTask(final Priority priority, final long subOrder, final long stream, final PositionedTask positionedTask,
                           final PrioritisedExecutor executor) {
            this.priority = priority;
            this.subOrder = subOrder;
            this.stream = stream;
            this.positionedTask = positionedTask;
            this.prioritisedTask = executor.createTask(this, priority, subOrder, stream);
        }

        PrioritisedExecutor.PrioritisedTask swapTask(final PrioritisedExecutor executor) {
            final PrioritisedExecutor.PrioritisedTask ret = this.prioritisedTask;

            this.prioritisedTask = executor.createTask(this, this.priority, this.subOrder, this.stream);

            return ret;
        }

        @Override
        public void run() {
            final PositionedTask task = this.positionedTask;

            if (!task.tryMarkExecuting(this)) {
                return;
            }

            try {
                task.run.run();
            } finally {
                task.finishTask(this);
            }
        }

        @Override
        public int compareTo(final QueuedTask other) {
            final int priorityCompare = this.priority.priority - other.priority.priority;
            if (priorityCompare != 0) {
                return priorityCompare;
            }

            if (this.positionedTask.areaDependentQueue.orderByLowerStream) {
                final int streamCompare = Long.compare(this.stream, other.stream);
                if (streamCompare != 0) {
                    return streamCompare;
                }
            }

            final int subOrderCompare = Long.compare(this.subOrder, other.subOrder);
            if (subOrderCompare != 0) {
                return subOrderCompare;
            }

            // break ties
            return Long.signum(this.positionedTask.taskId - other.positionedTask.taskId);
        }
    }
}
