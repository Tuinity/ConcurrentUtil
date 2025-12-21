package ca.spottedleaf.concurrentutil.scheduler;

import ca.spottedleaf.concurrentutil.util.TimeUtil;

public abstract class Scheduler {

    /**
     * Returns all threads in the scheduler that are alive.
     */
    public abstract Thread[] getAliveThreads();

    /**
     * Returns all threads in the scheduler which are allocated for executing tasks.
     */
    public abstract Thread[] getCoreThreads();

    /**
     * Attempts to prevent further execution of tasks.
     */
    public abstract void halt();

    /**
     * Waits until all threads in this pool have shutdown, or until the specified time has passed.
     * @param msToWait Maximum time to wait.
     * @return {@code false} if the maximum time passed, {@code true} otherwise.
     */
    public abstract boolean join(final long msToWait);

    /**
     * Waits until all threads in this pool have shutdown, or until the specified time has passed.
     * @param msToWait Maximum time to wait.
     * @return {@code false} if the maximum time passed, {@code true} otherwise.
     * @throws InterruptedException If this thread is interrupted.
     */
    public abstract boolean joinInterruptable(final long msToWait) throws InterruptedException;

    /**
     * Schedules the specified task to be executed on this thread pool.
     * @param tick Specified task
     * @throws IllegalStateException If the task is already scheduled, or if the task start is not set
     * @see SchedulableTick
     * @see TimeUtil#DEADLINE_NOT_SET
     */
    public abstract void schedule(final SchedulableTick tick);

    /**
     * Indicates that intermediate tasks are available to be executed by the task.
     * @param tick The specified task
     * @see SchedulableTick
     */
    public abstract void notifyTasks(final SchedulableTick tick);

    /**
     * Returns {@code false} if the task is not scheduled or is cancelled, returns {@code true} if the task was
     * cancelled by this thread
     */
    public abstract boolean cancel(final SchedulableTick tick);

}
