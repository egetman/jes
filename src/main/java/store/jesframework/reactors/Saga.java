package store.jesframework.reactors;

import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import javax.annotation.Nonnull;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import store.jesframework.Event;
import store.jesframework.JEventStore;
import store.jesframework.common.SagaFailure;
import store.jesframework.lock.Lock;
import store.jesframework.offset.Offset;
import store.jesframework.util.DaemonThreadFactory;

import static java.lang.Runtime.getRuntime;
import static java.util.concurrent.Executors.newFixedThreadPool;

/**
 * Note:Sagas are WIP.
 */

@Slf4j
public class Saga extends Reactor {

    private final ExecutorService workers = newFixedThreadPool(
            getRuntime().availableProcessors(),
            new DaemonThreadFactory(getClass().getSimpleName())
    );

    public Saga(@Nonnull JEventStore store, @Nonnull Offset offset, @Nonnull Lock lock) {
        super(store, offset, new BlockingPollingTrigger(lock));
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

    @Override
    public void close() {
        super.close();
        workers.shutdown();
    }

}
