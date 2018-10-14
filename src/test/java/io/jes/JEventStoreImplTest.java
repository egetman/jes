package io.jes;

import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.function.UnaryOperator;
import javax.sql.DataSource;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.jes.provider.FancyStuff;
import io.jes.provider.JdbcStoreProvider;
import io.jes.provider.SampleEvent;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

class JEventStoreImplTest {

    private final JEventStore source;
    private final JEventStore target;

    JEventStoreImplTest() {
        final DataSource sourceDataSource = FancyStuff.newDataSource("source");
        final DataSource targetDataSource = FancyStuff.newDataSource("target");

        this.source = new JEventStoreImpl(new JdbcStoreProvider<>(sourceDataSource, byte[].class));
        this.target = new JEventStoreImpl(new JdbcStoreProvider<>(targetDataSource, byte[].class));
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
        Assertions.assertIterableEquals(events, actual);

        final Collection<Event> empty = target.readBy(uuid);
        Assertions.assertTrue(empty.isEmpty());

        source.copyTo(target);
        final Collection<Event> transferred = target.readBy(uuid);
        Assertions.assertIterableEquals(events, transferred);
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
        Assertions.assertIterableEquals(events, actual);

        final Collection<Event> empty = target.readBy(uuid);
        Assertions.assertTrue(empty.isEmpty());

        // should change first 2 uuid and leave the rest
        final UUID newUuid = UUID.randomUUID();
        final UuidChanger handler = new UuidChanger(uuid, newUuid, 2);

        source.copyTo(target, handler);

        final Collection<Event> modified = target.readBy(newUuid);
        Assertions.assertIterableEquals(asList(
                new SampleEvent("FOO", newUuid, 0),
                new SampleEvent("BAR", newUuid, 1)
        ), modified);

        final Collection<Event> notModified = target.readBy(uuid);
        Assertions.assertIterableEquals(singletonList(new SampleEvent("BAZ", uuid, 0)), notModified);
    }

    private static class UuidChanger implements UnaryOperator<Event> {

        private int changed;
        private int changeCount;
        private final UUID oldUuid;
        private final UUID newUuid;

        private UuidChanger(UUID oldUuid, UUID newUuid, int changeCount) {
            this.oldUuid = oldUuid;
            this.newUuid = newUuid;
            this.changeCount = changeCount;
        }

        @Override
        public Event apply(Event event) {
            if (event instanceof SampleEvent) {
                SampleEvent sampleEvent = (SampleEvent) event;
                if (oldUuid.equals(sampleEvent.uuid()) && changeCount > 0) {
                    changeCount--;
                    return new SampleEvent(sampleEvent.getName(), newUuid, changed++);
                } else if (changed != 0) {
                    return new SampleEvent(sampleEvent.getName(), sampleEvent.uuid(),
                            sampleEvent.expectedStreamVersion() - changed);
                }
            }
            return event;
        }
    }

}