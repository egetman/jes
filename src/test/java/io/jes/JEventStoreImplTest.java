package io.jes;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.function.UnaryOperator;
import javax.annotation.Nonnull;
import javax.sql.DataSource;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import io.jes.ex.EmptyEventStreamException;
import io.jes.internal.FancyStuff;
import io.jes.provider.JdbcStoreProvider;

import static io.jes.internal.Events.SampleEvent;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toSet;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.TestInstance.Lifecycle;

@TestInstance(Lifecycle.PER_CLASS)
class JEventStoreImplTest {

    private final JEventStore source;
    private final JEventStore target;

    JEventStoreImplTest() {
        final DataSource sourceDataSource = FancyStuff.newPostgresDataSource("source");
        final DataSource targetDataSource = FancyStuff.newPostgresDataSource("target");

        this.source = new JEventStore(new JdbcStoreProvider<>(sourceDataSource, byte[].class));
        this.target = new JEventStore(new JdbcStoreProvider<>(targetDataSource, byte[].class));
    }

    @AfterEach
    void clearEventStores() {
        clearEventStore(source);
        clearEventStore(target);
    }

    private void clearEventStore(@Nonnull JEventStore store) {
        final Set<UUID> uuids = store.readFrom(0).map(Event::uuid).filter(Objects::nonNull).collect(toSet());
        uuids.forEach(store::deleteBy);
    }

    @Test
    void shouldThrowEmptyEventStreamExceptionOnNonExistingStreamUuid() {
        assertThrows(EmptyEventStreamException.class, () -> source.readBy(UUID.randomUUID()));
    }

    @Test
    void shouldCopySourceEventStoreContentIntoTargetEventStore() {
        final UUID uuid = UUID.randomUUID();
        final List<Event> events = asList(
                new SampleEvent("FOO", uuid, 0),
                new SampleEvent("BAR", uuid, 1),
                new SampleEvent("BAZ", uuid, 2)
        );

        events.forEach(source::write);
        final Collection<Event> actual = source.readBy(uuid);
        assertIterableEquals(events, actual);

        assertThrows(EmptyEventStreamException.class, () -> target.readBy(uuid));

        source.copyTo(target);
        final Collection<Event> transferred = target.readBy(uuid);
        assertIterableEquals(events, transferred);
    }

    @Test
    void shouldCopySourceEventStoreContentIntoTargetEventStoreWithModification() {
        final UUID uuid = UUID.randomUUID();
        final List<Event> events = asList(
                new SampleEvent("FOO", uuid, 0),
                new SampleEvent("BAR", uuid, 1),
                new SampleEvent("BAZ", uuid, 2)
        );

        events.forEach(source::write);
        final Collection<Event> actual = source.readBy(uuid);
        assertIterableEquals(events, actual);

        assertThrows(EmptyEventStreamException.class, () -> target.readBy(uuid));

        // should change first 2 uuid and leave the rest
        final UUID newUuid = UUID.randomUUID();
        final UuidChanger handler = new UuidChanger(uuid, newUuid, 2);

        source.copyTo(target, handler);

        final Collection<Event> modified = target.readBy(newUuid);
        assertIterableEquals(asList(
                new SampleEvent("FOO", newUuid, 0),
                new SampleEvent("BAR", newUuid, 1)
        ), modified);

        final Collection<Event> notModified = target.readBy(uuid);
        assertIterableEquals(singletonList(new SampleEvent("BAZ", uuid, 0)), notModified);
    }

    @Test
    void shouldReadEventsWithOffsetIfStoreImplementSnapshotReader() {
        final UUID uuid = UUID.randomUUID();
        final List<Event> events = asList(
                new SampleEvent("FOO", uuid),
                new SampleEvent("BAR", uuid),
                new SampleEvent("BAZ", uuid)
        );
        events.forEach(source::write);

        final Collection<Event> actual = source.readBy(uuid, 2);
        assertIterableEquals(singletonList(new SampleEvent("BAZ", uuid)), actual);
    }

    private static class UuidChanger implements UnaryOperator<Event> {

        private int changed;
        private int changeCount;
        private final UUID oldUuid;
        private final UUID newUuid;

        private UuidChanger(@Nonnull UUID oldUuid, @Nonnull UUID newUuid, int changeCount) {
            this.oldUuid = oldUuid;
            this.newUuid = newUuid;
            this.changeCount = changeCount;
        }

        @Override
        public Event apply(Event event) {
            if (event instanceof SampleEvent) {
                final SampleEvent sampleEvent = (SampleEvent) event;
                final UUID uuid = sampleEvent.uuid();
                final String eventName = sampleEvent.getName();

                if (oldUuid.equals(uuid) && changeCount > 0) {
                    changeCount--;
                    return new SampleEvent(eventName, newUuid, changed++);
                } else if (changed != 0) {
                    return new SampleEvent(eventName, uuid, sampleEvent.expectedStreamVersion() - changed);
                }
            }
            return event;
        }
    }

}