package ca.spottedleaf.concurrentutil.scheduler;

import ca.spottedleaf.concurrentutil.util.TimeUtil;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;

/**
 * Represents a tickable task that can be scheduled into a scheduled thread pool.
 * <p>
 * A tickable task is expected to be self scheduled using the {@link #setScheduledStart(long)} during
 * every tick.
 * </p>
 * <p>
 * A tickable task can have intermediate tasks that can be executed before its tick method is run. Instead of
 * the scheduled thread pool parking in-between ticks, the scheduler may instead drain intermediate tasks from scheduled
 * tasks. The parsing of intermediate tasks allows the scheduler to take advantage of time in-between ticks to reduce the
 * intermediate task load from tasks once they begin ticking or to possibly reduce latency parsing intermediate tasks.
 * </p>
 * <p>
 * It is guaranteed that {@link #runTick()} and {@link #runTasks(BooleanSupplier)} are never
 * invoked in parallel.
 * </p>
 * <p>
 * It is required that when intermediate tasks are scheduled, that the scheduler's notify task function
 * is invoked for any scheduled task - otherwise, the scheduler may not parse intermediate tasks.
 * </p>
 */
public abstract class SchedulableTick {

    private static final AtomicLong ID_GENERATOR = new AtomicLong();
    public final long id = ID_GENERATOR.getAndIncrement();

    long scheduledStart = TimeUtil.DEADLINE_NOT_SET;

    private volatile Object state;

    final Object getState() {
        return this.state;
    }

    boolean setState(final Object state) {
        synchronized (this) {
            if (this.state != null) {
                return false;
            }
            this.state = state;
            return true;
        }
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
     * @return {@code true} if this task should continue to be scheduled, {@code false} otherwise.
     */
    public abstract boolean runTick();

    /**
     * Returns whether this task has any intermediate tasks that can be executed.
     */
    public abstract boolean hasTasks();

    /**
     * @return {@code true} if this task should continue to be scheduled, {@code false} otherwise.
     */
    public abstract boolean runTasks(final BooleanSupplier canContinue);

    @Override
    public String toString() {
        return "SchedulableTick:{" +
                "class=" + this.getClass().getName() + "," +
                "state=" + this.getState() + ","
                + "}";
    }
}
