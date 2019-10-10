package store.jesframework.reactors;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import store.jesframework.AggregateStore;
import store.jesframework.JEventStore;
import store.jesframework.common.ContextUpdated;
import store.jesframework.internal.Events;
import store.jesframework.lock.Lock;
import store.jesframework.offset.InMemoryOffset;
import store.jesframework.offset.Offset;
import store.jesframework.provider.InMemoryStoreProvider;
import store.jesframework.provider.StoreProvider;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

@Slf4j
class StatefullSagaTest {

    @Test
    void statelessSagaShouldThrowExceptionOnContextAccess() {
        final JEventStore store = new JEventStore(mock(StoreProvider.class));
        final Offset offset = mock(Offset.class);
        final Lock lock = mock(Lock.class);

        try (StatefullSaga saga = new StatefullSaga(store, offset, lock)) {
            Assertions.assertThrows(IllegalStateException.class, saga::getContext);
        }
    }

    @Test
    @SneakyThrows
    void statefullSagasShouldSyncState() {
        final JEventStore store = new JEventStore(new InMemoryStoreProvider());
        final AggregateStore aggregateStore = new AggregateStore(store);
        final Offset offset = new InMemoryOffset();
        final Lock lock = mock(Lock.class);

        // pretend there is no synchronization at all
        doAnswer(invocation -> {
            invocation.getArgument(1, Runnable.class).run();
            return null;
        }).when(lock).doExclusively(anyString(), any(Runnable.class));

        try (StatefullSaga first = new StatefullSaga(aggregateStore, offset, lock, 1);
             StatefullSaga second = new StatefullSaga(aggregateStore, offset, lock, 1)) {

            store.write(new Events.SampleEvent("name"));

            first.latch.await(1, TimeUnit.SECONDS);
            second.latch.await(1, TimeUnit.SECONDS);

            Assertions.assertEquals(1, (Integer) first.getContext().get("name"));
            Assertions.assertEquals(1, (Integer) second.getContext().get("name"));
        }
    }

    private static class StatefullSaga extends Saga {

        private final CountDownLatch latch;

        StatefullSaga(@Nonnull JEventStore store, @Nonnull Offset offset, @Nonnull Lock lock) {
            super(store, offset, lock);
            latch = new CountDownLatch(0);
        }

        StatefullSaga(@Nonnull AggregateStore aggregateStore, @Nonnull Offset offset, @Nonnull Lock lock,
                      int stateCanges) {
            super(aggregateStore, offset, lock);
            latch = new CountDownLatch(stateCanges);
        }

        @ReactsOn
        @SuppressWarnings("unused")
        private void handle(@Nonnull Events.SampleEvent event) {
            final Context context = getContext();
            final boolean success = context.set(event.getName(), 1);
            if (!success) {
                log.warn("Failed to initialize context for key #{}: already initialized", event.getName());
            }
        }

        // just to wait until state sync
        @ReactsOn
        @SuppressWarnings("unused")
        void handle(ContextUpdated event) {
            latch.countDown();
        }
    }
}
