package ca.spottedleaf.concurrentutil.util;

import java.lang.invoke.VarHandle;

public final class LazyRunnable implements Runnable {

    private volatile Runnable toRun;
    private static final VarHandle TO_RUN_HANDLE = ConcurrentUtil.getVarHandle(LazyRunnable.class, "toRun", Runnable.class);

    public LazyRunnable() {}

    public LazyRunnable(final Runnable run) {
        TO_RUN_HANDLE.setRelease(this, run);
    }

    public void setRunnable(final Runnable run) {
        final Runnable prev = (Runnable)TO_RUN_HANDLE.compareAndExchange(this, (Runnable)null, run);
        if (prev != null) {
            throw new IllegalStateException("Runnable already set");
        }
    }

    @Override
    public void run() {
        ((Runnable)TO_RUN_HANDLE.getVolatile(this)).run();
    }
}
