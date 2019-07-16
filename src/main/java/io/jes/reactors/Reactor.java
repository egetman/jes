package io.jes.reactors;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.function.Consumer;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

import io.jes.Command;
import io.jes.Event;
import io.jes.JEventStore;
import io.jes.bus.CommandBus;
import io.jes.offset.Offset;
import io.jes.util.DaemonThreadFactory;
import lombok.extern.slf4j.Slf4j;

import static io.jes.reactors.ReactorUtils.ensureReactsOnHasEventParameter;
import static io.jes.reactors.ReactorUtils.ensureReactsOnHasOneParameter;
import static io.jes.reactors.ReactorUtils.ensureReactsOnHasVoidReturnType;
import static io.jes.reactors.ReactorUtils.getAllReactsOnMethods;
import static io.jes.reactors.ReactorUtils.invokeReactsOn;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@Slf4j
abstract class Reactor implements AutoCloseable {

    private static final long DELAY_MS = 500;

    final Offset offset;
    final String key = getClass().getName();

    private final CommandBus bus;
    private final JEventStore store;
    private final ThreadFactory factory = new DaemonThreadFactory(getClass().getSimpleName());
    private final ScheduledExecutorService executor = newSingleThreadScheduledExecutor(factory);
    private final Map<Class<? extends Event>, Consumer<? super Event>> reactors = new HashMap<>();

    Reactor(@Nonnull JEventStore store, @Nonnull Offset offset, @Nonnull CommandBus bus) {
        this.bus = Objects.requireNonNull(bus, "CommandBus must not be null");
        this.offset = Objects.requireNonNull(offset, "Offset must not be null");
        this.store = Objects.requireNonNull(store, "Event store must not be null");

        this.reactors.putAll(readReactors());
        executor.scheduleWithFixedDelay(this::tailStore, DELAY_MS, DELAY_MS, MILLISECONDS);
    }

    @Nonnull
    private Map<Class<? extends Event>, Consumer<? super Event>> readReactors() {
        final Set<Method> methods = getAllReactsOnMethods(getClass());
        log.debug("Resolved {} reactor methods", methods.size());
        final Map<Class<? extends Event>, Consumer<? super Event>> eventToConsumer = new HashMap<>();
        for (Method method : methods) {
            log.debug("Start verification of '{}'", method);
            ensureReactsOnHasOneParameter(method);
            ensureReactsOnHasVoidReturnType(method);
            ensureReactsOnHasEventParameter(method);
            log.debug("Verification of '{}' complete", method);

            method.setAccessible(true);
            @SuppressWarnings("unchecked")
            final Class<? extends Event> eventType = (Class<? extends Event>) method.getParameterTypes()[0];
            eventToConsumer.put(eventType, (event -> invokeReactsOn(method, this, event)));
        }

        return eventToConsumer;
    }

    // think of better solution for tailing. mb CDC (https://github.com/debezium/debezium) for db backed stores?
    void tailStore() {
        try (Stream<Event> eventStream = store.readFrom(offset.value(key))) {
            eventStream.forEach(event -> {
                final Consumer<? super Event> consumer = reactors.get(event.getClass());
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

    @SuppressWarnings("WeakerAccess")
    protected void dispatch(@Nonnull Command command) {
        bus.dispatch(Objects.requireNonNull(command));
    }
}
