package ca.spottedleaf.concurrentutil.executor.thread;

import ca.spottedleaf.concurrentutil.list.COWArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Arrays;
import java.util.function.Consumer;

public final class StreamOrderedThreadPool {

    public static final long DEFAULT_DIVISION_TIME_SLICE = (long)(15.0e6); // 15ms

    private static final Logger LOGGER = LoggerFactory.getLogger(StreamOrderedThreadPool.class);

    private final Consumer<Thread> threadModifier;

    private final COWArrayList<Division> divisions = new COWArrayList<>(Division.class);
    private final COWArrayList<WorkerThread> threads = new COWArrayList<>(WorkerThread.class);
    private final COWArrayList<WorkerThread> aliveThreads = new COWArrayList<>(WorkerThread.class);

    private final long groupTimeSliceNS;

    private boolean shutdown;

    public StreamOrderedThreadPool(final long groupTimeSliceNS, final Consumer<Thread> threadModifier) {
        this.threadModifier = threadModifier;

        if (threadModifier == null) {
            throw new NullPointerException("Thread factory may not be null");
        }
        this.groupTimeSliceNS = groupTimeSliceNS;
    }

    public Division createDivision() {
        throw new UnsupportedOperationException();
    }

    public Thread[] getAliveThreads() {
        final WorkerThread[] threads = this.aliveThreads.getArray();

        return Arrays.copyOf(threads, threads.length, Thread[].class);
    }

    public Thread[] getCoreThreads() {
        final WorkerThread[] threads = this.threads.getArray();

        return Arrays.copyOf(threads, threads.length, Thread[].class);
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

    protected final boolean join(final long msToWait, final boolean interruptable) throws InterruptedException {
        final long nsToWait = msToWait * (1000 * 1000);
        final long start = System.nanoTime();
        final long deadline = start + nsToWait;
        boolean interrupted = false;
        try {
            for (final WorkerThread thread : this.aliveThreads.getArray()) {
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

    public final class Division {

    }

    private final class WorkerThread extends PrioritisedQueueExecutorThread {

        public WorkerThread() {
            super(null);
        }

        @Override
        protected boolean pollTasks() {
            throw new UnsupportedOperationException();
        }

        @Override
        protected void die() {
            StreamOrderedThreadPool.this.die(this);
        }
    }
}
