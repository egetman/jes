package store.jesframework.reactors;

import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;

import lombok.extern.slf4j.Slf4j;
import store.jesframework.util.DaemonThreadFactory;

import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

@Slf4j
class PollingTrigger implements Trigger {

    private static final long DELAY_MS = 100;

    private final ThreadFactory factory = new DaemonThreadFactory(getClass().getSimpleName());
    private final ScheduledExecutorService executor = newSingleThreadScheduledExecutor(factory);

    @Override
    public void onChange(@Nonnull String key, @Nonnull Runnable runnable) {
        Objects.requireNonNull(key, "Key must not be null");
        Objects.requireNonNull(runnable, "Runnable action must not be null");
        executor.scheduleWithFixedDelay(() -> {
            try {
                runnable.run();
            } catch (Exception e) {
                log.error("Failed to run task:", e);
            }
        }, DELAY_MS, DELAY_MS, TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() {
        executor.shutdown();
    }

    long getDelay() {
        return DELAY_MS;
    }
}
