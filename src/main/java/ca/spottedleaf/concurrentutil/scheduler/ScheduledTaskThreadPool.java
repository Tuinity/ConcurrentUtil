package ca.spottedleaf.concurrentutil.scheduler;

import ca.spottedleaf.concurrentutil.list.COWArrayList;
import ca.spottedleaf.concurrentutil.util.ConcurrentUtil;
import java.lang.invoke.VarHandle;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;

public final class ScheduledTaskThreadPool {

    public static final long DEADLINE_NOT_SET = Long.MIN_VALUE;

    private final ThreadFactory threadFactory;

    private final COWArrayList<TickThreadRunner> coreThreads = new COWArrayList<>(TickThreadRunner.class);
    private final COWArrayList<TickThreadRunner> aliveThreads = new COWArrayList<>(TickThreadRunner.class);

    private boolean shutdown;

    public ScheduledTaskThreadPool(final ThreadFactory threadFactory) {
        this.threadFactory = threadFactory;

        if (threadFactory == null) {
            throw new NullPointerException("Null thread factory");
        }
    }

    /**
     * Attempts to prevent further execution of tasks.
     */
    public void halt() {
        synchronized (this) {
            this.shutdown = true;
        }

        for (final TickThreadRunner runner : this.coreThreads.getArray()) {
            //runner.halt(); // TODO
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
            for (final TickThreadRunner runner : this.aliveThreads.getArray()) {
                final Thread thread = null; // TODO
                for (;;) {
                    if (!thread.isAlive()) {
                        break;
                    }
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
     * Schedules the specified task to be executed on this thread pool.
     * @param tick Specified task
     * @throws IllegalStateException If the task is already scheduled
     * @see SchedulableTick
     */
    public void schedule(final SchedulableTick tick) {

    }

    /**
     * Indicates that intermediate tasks are available to be executed by the task.
     * @param tick The specified task
     * @see SchedulableTick
     */
    public void notifyTasks(final SchedulableTick tick) {

    }

    /**
     * Returns {@code false} if the task is not scheduled or is cancelled, returns {@code true} if the task was
     * cancelled by this thread
     */
    public boolean cancel(final SchedulableTick tick) {
        // TODO
        throw new UnsupportedOperationException();
    }

    /**
     * Represents a tickable task that can be scheduled into a {@link ScheduledTaskThreadPool}.
     * <p>
     * A tickable task is expected to run on a fixed interval, which is determined by
     * the {@link ScheduledTaskThreadPool}.
     * </p>
     * <p>
     * A tickable task can have intermediate tasks that can be executed before its tick method is ran. Instead of
     * the {@link ScheduledTaskThreadPool} parking in-between ticks, the scheduler will instead drain
     * intermediate tasks from scheduled tasks. The parsing of intermediate tasks allows the scheduler to take
     * advantage of downtime to reduce the intermediate task load from tasks once they begin ticking.
     * </p>
     * <p>
     * It is guaranteed that {@link #runTick()} and {@link #runTasks(BooleanSupplier)} are never
     * invoked in parallel.
     * It is required that when intermediate tasks are scheduled, that {@link ScheduledTaskThreadPool#notifyTasks(SchedulableTick)}
     * is invoked for any scheduled task - otherwise, {@link #runTasks(BooleanSupplier)} may not be invoked to
     * parse intermediate tasks.
     * </p>
     */
    public static abstract class SchedulableTick {
        private static final AtomicLong ID_GENERATOR = new AtomicLong();
        public final long id = ID_GENERATOR.getAndIncrement();

        private long scheduledStart = DEADLINE_NOT_SET;

        private static final int STATE_UNSCHEDULED       = 1 << 0;
        private volatile int state = STATE_UNSCHEDULED;
        private static final VarHandle STATE_HANDLE = ConcurrentUtil.getVarHandle(SchedulableTick.class, "state", int.class);

        private int getStateVolatile() {
            return (int)STATE_HANDLE.getVolatile(this);
        }

        private void setStateVolatile(final int value) {
            STATE_HANDLE.setVolatile(this, value);
        }

        private int compareAndExchangeStateVolatile(final int expect, final int update) {
            return (int)STATE_HANDLE.compareAndExchange(this, expect, update);
        }

        protected final long getScheduledStart() {
            return this.scheduledStart;
        }

        /**
         * If this task is scheduled, then this may only be invoked during {@link #runTick()}
         */
        protected final void setScheduledStart(final long value) {
            this.scheduledStart = value;
        }

        /**
         * Executes the tick.
         * <p>
         * It is the callee's responsibility to invoke {@link #setScheduledStart(long)} to adjust the start of
         * the next tick.
         * </p>
         * @return {@code true} if the task should continue to be scheduled, {@code false} otherwise.
         */
        public abstract boolean runTick();

        /**
         * Returns whether this task has any intermediate tasks that can be executed.
         */
        public abstract boolean hasTasks();

        /**
         * @return {@code true} if the task should continue to be scheduled, {@code false} otherwise.
         */
        public abstract boolean runTasks(final BooleanSupplier canContinue);

        @Override
        public String toString() {
            return "SchedulableTick:{" +
                    "class=" + this.getClass().getName() + "," +
                    "state=" + this.state + ","
                    + "}";
        }
    }


    private static final class TickThreadRunner implements Runnable {

        private final ScheduledTaskThreadPool scheduler;
        private final long id;

        public TickThreadRunner(final ScheduledTaskThreadPool scheduler, final long id) {
            this.scheduler = scheduler;
            this.id = id;
        }

        private void begin() {}

        private void doRun() {
            while (this.mainLoop());
        }

        private boolean mainLoop() {
            // TODO
            throw new UnsupportedOperationException();
        }

        private void die() {}

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
