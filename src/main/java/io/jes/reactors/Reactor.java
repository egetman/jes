package io.jes.reactors;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.function.Consumer;
import javax.annotation.Nonnull;

import io.jes.Event;
import io.jes.JEventStore;
import io.jes.offset.Offset;
import io.jes.util.DaemonThreadFactory;
import lombok.extern.slf4j.Slf4j;

import static io.jes.reactors.HandlerUtils.ensureHandlerHasEventParameter;
import static io.jes.reactors.HandlerUtils.ensureHandlerHasOneParameter;
import static io.jes.reactors.HandlerUtils.ensureHandlerHasVoidReturnType;
import static io.jes.reactors.HandlerUtils.getAllHandlerMethods;
import static io.jes.reactors.HandlerUtils.invokeHandler;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@Slf4j
abstract class Reactor implements AutoCloseable {

    private static final long DELAY_MS = 500;

    final Offset offset;
    final String key = getClass().getName();

    private final JEventStore store;
    private final ThreadFactory factory = new DaemonThreadFactory(getClass().getSimpleName());
    private final ScheduledExecutorService executor = newSingleThreadScheduledExecutor(factory);
    private final Map<Class<? extends Event>, Consumer<? super Event>> handlers = new HashMap<>();

    Reactor(@Nonnull JEventStore store, @Nonnull Offset offset) {
        this.store = Objects.requireNonNull(store, "Event store must not be null");
        this.offset = Objects.requireNonNull(offset, "Offset must not be null");

        this.handlers.putAll(readHandlers());
        executor.scheduleWithFixedDelay(this::tailStore, DELAY_MS, DELAY_MS, MILLISECONDS);
    }

    @Nonnull
    private Map<Class<? extends Event>, Consumer<? super Event>> readHandlers() {
        final Set<Method> methods = getAllHandlerMethods(getClass());
        log.debug("Resolved {} handler methods", methods.size());
        final Map<Class<? extends Event>, Consumer<? super Event>> eventToConsumer = new HashMap<>();
        for (Method method : methods) {
            log.debug("Start verification of '{}'", method);
            ensureHandlerHasOneParameter(method);
            ensureHandlerHasVoidReturnType(method);
            ensureHandlerHasEventParameter(method);
            log.debug("Verification of '{}' complete", method);

            method.setAccessible(true);
            @SuppressWarnings("unchecked")
            final Class<? extends Event> eventType = (Class<? extends Event>) method.getParameterTypes()[0];
            eventToConsumer.put(eventType, (event -> invokeHandler(method, this, event)));
        }

        return eventToConsumer;
    }

    // think of better solution for tailing. mb CDC (https://github.com/debezium/debezium) for db backed stores?
    void tailStore() {
        try {
            store.readFrom(offset.value(key)).forEach(event -> {
                final Consumer<? super Event> consumer = handlers.get(event.getClass());
                if (consumer != null) {
                    consumer.accept(event);
                    log.trace("Handled {}", event.getClass().getSimpleName());
                }
                offset.increment(key);
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
