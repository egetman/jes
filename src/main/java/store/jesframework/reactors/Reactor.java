package store.jesframework.reactors;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

import store.jesframework.Event;
import store.jesframework.JEventStore;
import store.jesframework.offset.Offset;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
abstract class Reactor implements AutoCloseable {

    final Offset offset;
    final JEventStore store;
    private final Trigger trigger;

    @Getter(value = AccessLevel.PROTECTED)
    private final String key = getClass().getName();
    private final Map<Class<? extends Event>, Consumer<? super Event>> reactors = new HashMap<>();

    Reactor(@Nonnull JEventStore store, @Nonnull Offset offset) {
        this(store, offset, new PollingTrigger());
    }

    Reactor(@Nonnull JEventStore store, @Nonnull Offset offset, @Nonnull Trigger trigger) {
        this.store = Objects.requireNonNull(store, "Event store must not be null");
        this.offset = Objects.requireNonNull(offset, "Offset must not be null");

        this.trigger = Objects.requireNonNull(trigger, "Trigger must not be null");
        this.reactors.putAll(readReactors());

        trigger.onChange(this::tailStore);
    }

    @Nonnull
    private Map<Class<? extends Event>, Consumer<? super Event>> readReactors() {
        final Set<Method> methods = ReactorUtils.getAllReactsOnMethods(getClass());
        log.debug("Resolved {} reactor methods", methods.size());
        final Map<Class<? extends Event>, Consumer<? super Event>> eventToConsumer = new HashMap<>();
        for (Method method : methods) {
            log.debug("Start verification of '{}'", method);
            ReactorUtils.ensureReactsOnHasOneParameter(method);
            ReactorUtils.ensureReactsOnHasVoidReturnType(method);
            ReactorUtils.ensureReactsOnHasEventParameter(method);
            log.debug("Verification of '{}' complete", method);

            method.setAccessible(true);
            @SuppressWarnings("unchecked")
            final Class<? extends Event> eventType = (Class<? extends Event>) method.getParameterTypes()[0];
            eventToConsumer.put(eventType, (event -> ReactorUtils.invokeReactsOn(method, this, event)));
        }

        return eventToConsumer;
    }

    // think of better solution for tailing. mb CDC (https://github.com/debezium/debezium) for db backed stores?
    void tailStore() {
        final LongAdder counter = new LongAdder();
        final long offsetValue = offset.value(getKey());
        log.trace("Current offset value: {} for {}", offsetValue, getKey());

        try (Stream<Event> eventStream = store.readFrom(offsetValue)) {
            eventStream.forEach(event -> {
                final Consumer<? super Event> consumer = reactors.get(event.getClass());
                if (consumer != null) {
                    accept(offsetValue + counter.longValue(), event, consumer);
                }
                counter.increment();
            });
        } catch (Exception e) {
            // we must not stop to try read store, if any exception happens
            log.error("Exception during event store tailing:", e);
        } finally {
            final long processedCount = counter.longValue();
            offset.add(getKey(), processedCount);
            log.trace("Offset increased for: {} by {}", processedCount, getKey());
        }
    }

    @SuppressWarnings({"unused"})
    protected void accept(long offset, @Nonnull Event event, @Nonnull Consumer<? super Event> consumer) {
        consumer.accept(event);
        log.trace("Handled {}", event.getClass().getSimpleName());
    }

    @Override
    @SneakyThrows
    public void close() {
        trigger.close();
        log.debug("{} closed", getKey());
    }
}
