package io.jes.reactors;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;

import io.jes.JEventStore;
import io.jes.common.FancyEvent;
import io.jes.common.ProcessingStarted;
import io.jes.common.ProcessingTerminated;
import io.jes.common.SampleEvent;
import io.jes.lock.InMemoryLockManager;
import io.jes.offset.Offset;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@SuppressWarnings("unused")
class SampleProjector extends Projector {

    private Projection projection;
    private CountDownLatch endStreamLatch = new CountDownLatch(1);
    private final CountDownLatch started = new CountDownLatch(1);

    SampleProjector(@Nonnull JEventStore store, @Nonnull Offset offset) {
        super(store, offset, new InMemoryLockManager());
    }

    @Handler
    private void handle(@Nonnull ProcessingStarted event) {
        projection = new Projection();
        projection.totalProcessed++;
    }

    @Handler
    private void handle(@Nonnull SampleEvent event) {
        projection.name = event.getName();
        projection.totalProcessed++;
        projection.uniqueEventStreams.add(event.uuid());
    }

    @Handler
    private void handle(@Nonnull FancyEvent event) {
        projection.totalProcessed++;
        projection.uniqueEventStreams.add(event.uuid());
    }

    @Handler
    private void handle(@Nonnull ProcessingTerminated event) {
        projection.totalProcessed++;
        endStreamLatch.countDown();
    }

    @SneakyThrows
    Projection getProjection() {
        endStreamLatch.await(1, TimeUnit.SECONDS);
        return projection;
    }

    @Override
    void tailStore() {
        super.tailStore();
        started.countDown();
    }

    @SneakyThrows
    boolean isStarted() {
        return started.await(1, TimeUnit.SECONDS);
    }

    @Override
    public void recreate() {
        super.recreate();
        endStreamLatch = new CountDownLatch(1);
    }

    // this is just in-memory projection. But it can be any kind of projection like separate sql-db, or elastic etc.
    @Data
    static class Projection {

        private String name;
        private long totalProcessed;
        private final Set<UUID> uniqueEventStreams = new HashSet<>();

    }
}
