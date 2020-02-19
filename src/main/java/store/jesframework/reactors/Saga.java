package store.jesframework.reactors;

import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import store.jesframework.Aggregate;
import store.jesframework.AggregateStore;
import store.jesframework.Event;
import store.jesframework.JEventStore;
import store.jesframework.common.ContextUpdated;
import store.jesframework.common.SagaFailure;
import store.jesframework.ex.VersionMismatchException;
import store.jesframework.lock.Lock;
import store.jesframework.offset.Offset;
import store.jesframework.util.DaemonThreadFactory;

import static java.lang.Runtime.getRuntime;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static store.jesframework.reactors.ReactorUtils.uuidByKey;

/**
 * Note:Sagas are WIP.
 */

@Slf4j
public class Saga extends Reactor {

    private static final long STATE_REFRESH_DELAY = 100;

    private final Context context;
    private final AggregateStore aggregateStore;
    private final UUID sagaUuid = uuidByKey(getKey());

    private final DaemonThreadFactory factory = new DaemonThreadFactory(getClass().getSimpleName());
    private final ExecutorService workers = newFixedThreadPool(getRuntime().availableProcessors(), factory);
    private final ScheduledExecutorService refresher = newSingleThreadScheduledExecutor(factory);

    public Saga(@Nonnull JEventStore store, @Nonnull Offset offset, @Nonnull Lock lock) {
        // stateless saga instance, no context will be tracked.
        super(store, offset, new BlockingPollingTrigger(lock));
        this.context = null;
        this.aggregateStore = null;
    }

    public Saga(@Nonnull AggregateStore aggregateStore, @Nonnull Offset offset, @Nonnull Lock lock) {
        // stateful saga instance, context will be tracked and refreshed every #STATE_REFRESH_DELAY ms.
        super(aggregateStore.unwrap(), offset, new BlockingPollingTrigger(lock));
        this.aggregateStore = aggregateStore;
        this.context = this.aggregateStore.readBy(sagaUuid, new Context());
        // refresh state every 100 ms
        refresher.scheduleWithFixedDelay(() -> aggregateStore.readBy(sagaUuid, context),
                STATE_REFRESH_DELAY, STATE_REFRESH_DELAY, TimeUnit.MILLISECONDS
        );
    }

    @Override
    @SneakyThrows
    protected void accept(long offset, @Nonnull Event event, @Nonnull Consumer<? super Event> consumer) {
        workers.execute(() -> {
            try {
                super.accept(offset, event, consumer);
            } catch (Exception e) {
                log.error("Failed to handle event {}", event, e);
                store.write(new SagaFailure(event, getKey(), offset, e.getMessage()));
            }
        });
    }

    /**
     * Returns the shared context for all sagas instances identified by uuid made of the {@literal #getKey()}.
     *
     * @return shared state context.
     * @throws IllegalStateException if this saga wasn't instantiated in stateful mode.
     */
    @SuppressWarnings({"unused", "WeakerAccess"})
    protected Context getContext() {
        if (aggregateStore == null) {
            throw new IllegalStateException(
                    "Context available only for stateful sagas. Use constructor #(AggregateStore, Offset, Lock)");
        }
        return context;
    }

    @Override
    public void close() {
        super.close();
        workers.shutdown();
        refresher.shutdown();
    }

    protected class Context extends Aggregate {

        private final Map<String, Object> state = new ConcurrentHashMap<>();

        Context() {
            this.uuid = sagaUuid;
            registerApplier(ContextUpdated.class, this::apply);
        }

        private void apply(@Nonnull ContextUpdated event) {
            state.compute(event.getKey(), (key, value) -> event.getValue());
        }

        /**
         * Sets the given value into the saga context. This call is equivalent to {@code compareAndSet(key, null,
         * value)}.
         *
         * <p>Note: {@literal set} and {@literal compareAndSet} operations are async, so any changes may not
         * be seen immediately. Context state refreshed every {@link Saga#STATE_REFRESH_DELAY} ms.</p>
         *
         * <p>If {@code null} value supplied, given {@code key} will be removed from context.</p>
         *
         * @param key   the key whose associated value is to be set.
         * @param value the value to be set.
         * @return {@code true} if given {@code key} hasn't any mappings before and the {@literal set} operation
         *      completes successfully, false otherwise.
         */
        public boolean set(@Nonnull String key, @Nullable Object value) {
            return compareAndSet(key, null, value);
        }

        /**
         * Sets the given {@code next} value into the saga context if the {@code prev} value is equal to the previously
         * associated value with the given {@code key}.
         *
         * <p>Note: {@literal set} and {@literal compareAndSet} operations are async, so any changes may not
         * be seen immediately. Context state refreshed every {@link Saga#STATE_REFRESH_DELAY} ms.</p>
         *
         * <p>If {@code null} {@literal next} value supplied, given {@code key} will be removed from context.</p>
         * Typical use pattern (in the saga instance):
         *
         * <pre class="code"><code class="java">
         *
         *  &#064;ReactsOn
         *  void handle(SomeEvent event) {
         *      final Context context = getContext();
         *      // any type that you want
         *      final Object currentValue = computeNewState();
         *      Object oldValue = context.get("myParam");
         *
         *      while(!context.compareAndSet("myParam", oldValue, currentValue)) {
         *          // failed to update context - old value could be changed by another instance
         *          // you need to refresh prev value
         *          oldValue = context.get("myParam");
         *      }
         *  }
         * </code></pre>
         *
         * @param key  the key whose associated value is to be set.
         * @param prev the expected previous value for the {@code key}.
         * @param next the new value to be set.
         * @return {@code true} if successful. False return indicates that the actual value was not equal to the
         *      expected value.
         */
        @SuppressWarnings("WeakerAccess")
        public boolean compareAndSet(@Nonnull String key, @Nullable Object prev, @Nullable Object next) {
            Objects.requireNonNull(key, "Key must not be null");
            long cachedVersion = streamVersion();
            Object probe = state.get(key);

            // try to write state change until the previous value is an actual, even if the event stream version changed
            while (Objects.equals(probe, prev)) {
                try {
                    aggregateStore.write(new ContextUpdated(sagaUuid, key, next, cachedVersion));
                    return true;
                } catch (VersionMismatchException e) {
                    log.trace("Failed to update saga context: {}", e.toString());
                    // wait until a refresh triggered (to avoid too much useless spinning)
                    try {
                        TimeUnit.MILLISECONDS.sleep(STATE_REFRESH_DELAY);
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                    }
                    cachedVersion = streamVersion();
                    // if the state changed for that concrete key - leave the decision to a caller
                    probe = state.get(key);
                }
            }
            return false;
        }

        /**
         * Returns the value to which the specified key is mapped, or {@code null} if this context contains no mapping
         * for the key.
         *
         * @param key the key whose associated value is to be returned.
         * @param <T> the type of mapped values.
         * @return the value to which the specified key is mapped, or {@code null} if this context contains no mapping
         *      for the key.
         * @throws ClassCastException if the key is of an inappropriate type for this context.
         */
        @Nullable
        public <T> T get(@Nonnull String key) {
            Objects.requireNonNull(key, "Key must not be null");
            //noinspection unchecked
            return (T) state.get(key);
        }

    }

}
