package store.jesframework.reactors;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;

import store.jesframework.util.DaemonThreadFactory;

import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

class PollingTrigger implements Trigger {

    private static final long DELAY_MS = 100;

    private final ThreadFactory factory = new DaemonThreadFactory(getClass().getSimpleName());
    private final ScheduledExecutorService executor = newSingleThreadScheduledExecutor(factory);

    @Override
    public void onChange(@Nonnull Runnable runnable) {
        executor.scheduleWithFixedDelay(runnable, DELAY_MS, DELAY_MS, TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() {
        executor.shutdown();
    }

    long getDelay() {
        return DELAY_MS;
    }
}
