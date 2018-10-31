package io.jes;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import javax.annotation.Nonnull;

import io.jes.util.DaemonThreadFactory;
import lombok.extern.slf4j.Slf4j;

import static io.jes.util.Check.nonEmpty;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@Slf4j
abstract class Reactor implements AutoCloseable {

    private static final long DELAY_MS = 500;

    final LongAdder offset = new LongAdder();

    private final JEventStore store;
    private final ThreadFactory factory = new DaemonThreadFactory(getClass().getSimpleName());
    private final ScheduledExecutorService executor = newSingleThreadScheduledExecutor(factory);
    private final Map<Class<? extends Event>, Consumer<Event>> handlers = new HashMap<>();

    Reactor(@Nonnull JEventStore store) {
        this.store = Objects.requireNonNull(store, "Event store must not be null");
        this.handlers.putAll(handlers());
        // validate state
        validateHandlers(handlers);
        executor.scheduleWithFixedDelay(this::tailStore, DELAY_MS, DELAY_MS, MILLISECONDS);
    }

    @Nonnull
    @SuppressWarnings("WeakerAccess")
    protected abstract Map<Class<? extends Event>, Consumer<Event>> handlers();

    private void validateHandlers(@Nonnull Map<Class<? extends Event>, Consumer<Event>> handlers) {
        nonEmpty(handlers, () -> new IllegalArgumentException("You should register at least 1 handler"));
        for (Map.Entry<Class<? extends Event>, Consumer<Event>> entry : handlers.entrySet()) {
            Objects.requireNonNull(entry.getValue(), "Handler for " + entry.getKey() + " must not be null");
        }
    }

    void tailStore() {
        try {
            store.readFrom(offset.longValue()).forEach(event -> {
                final Consumer<Event> consumer = handlers.get(event.getClass());
                if (consumer != null) {
                    consumer.accept(event);
                }
                offset.increment();
            });
        } catch (Exception e) {
            // we must not stop to try read store, if any exception happens
            log.error("Exception during event store tailing:", e);
        }
    }

    @Override
    public void close() {
        executor.shutdown();
        log.debug("{} closed", getClass().getSimpleName());
    }
}
