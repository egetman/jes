package io.jes;

import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;
import java.util.function.UnaryOperator;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import io.jes.common.FancyEvent;
import io.jes.common.SampleEvent;
import io.jes.common.StreamMovedTo;
import io.jes.ex.EmptyEventStreamException;
import io.jes.ex.EventStreamRewriteUnsupportedException;
import io.jes.ex.EventStreamSplitUnsupportedException;
import io.jes.provider.JdbcStoreProvider;

import static io.jes.common.FancyStuff.newDataSource;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.UUID.randomUUID;
import static java.util.function.UnaryOperator.identity;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.TestInstance.Lifecycle;

@TestInstance(Lifecycle.PER_CLASS)
class UnsafeOpsTest {

    private final JEventStore store;
    private final UnsafeOps unsafeOps;

    UnsafeOpsTest() {
        store = new JEventStoreImpl(new JdbcStoreProvider<>(newDataSource(), byte[].class));
        unsafeOps = new UnsafeOps(store);
    }

    @Test
    void eventStreamToUniqUuidShouldCorrectlyFilterEvents() {
        final UUID uuid = randomUUID();
        assertNull(unsafeOps.eventStreamToUniqUuid(emptyList()));

        assertEquals(uuid, unsafeOps.eventStreamToUniqUuid(singletonList(new SampleEvent("", uuid))));
        assertEquals(uuid, unsafeOps.eventStreamToUniqUuid(asList(
                new SampleEvent("FOO", uuid),
                new SampleEvent("BAR", uuid)
        )));

        assertThrows(EventStreamSplitUnsupportedException.class, () -> unsafeOps.eventStreamToUniqUuid(asList(
                new SampleEvent("FOO"),
                new SampleEvent("BAR")
        )));
    }

    @Test
    void traverseAndReplaceShouldThrowNullPointerOnNullArguments() {
        //noinspection ConstantConditions
        assertThrows(NullPointerException.class, () -> unsafeOps.traverseAndReplace(null, identity()));

        final UUID uuid = randomUUID();
        store.write(new SampleEvent("FOO", uuid));
        //noinspection ConstantConditions
        assertThrows(NullPointerException.class, () -> unsafeOps.traverseAndReplace(uuid, null));
    }

    // empty stream exception will be thrown cause no stream with given uuid exists
    @Test
    void traverseAndReplaceShouldThrowEmptyEventStreamExceptionWhenEventsNotFoundByUuid() {
        assertThrows(EmptyEventStreamException.class, () -> unsafeOps.traverseAndReplace(randomUUID(), identity()));
    }

    @Test
    void traverseAndReplaceShouldThrowEmptyEventStreamExceptionWhenNoEventsProducedByHandler() {
        final SampleEvent event = new SampleEvent("FOO", randomUUID());
        store.write(event);

        assertThrows(EmptyEventStreamException.class, () -> unsafeOps.traverseAndReplace(event.uuid(), obj -> null));
    }

    @Test
    void traverseAndReplaceShouldThrowEventStreamSplitUnsupportedExceptionWhenMultipleEventsProducedByHandlerWithDifferentUuid() {
        final UUID uuid = randomUUID();
        final Collection<Event> source = asList(new SampleEvent("FOO", uuid), new SampleEvent("BAR", uuid));
        source.forEach(store::write);

        assertThrows(EventStreamSplitUnsupportedException.class, () -> unsafeOps.traverseAndReplace(uuid, event -> {
            final SampleEvent sampleEvent = (SampleEvent) event;
            return new SampleEvent(sampleEvent.getName(), randomUUID());
        }));
    }

    @Test
    void traverseAndReplaceShouldSuccessfullyChangeStreamContent() {
        // write source events for future processing
        final UUID uuid = randomUUID();
        final Collection<Event> source = asList(
                new SampleEvent("FOO", uuid),
                new SampleEvent("BAR", uuid),
                new SampleEvent("BAZ", uuid)
        );
        source.forEach(store::write);

        // we replace last event from original stream
        class StreamChanger implements UnaryOperator<Event> {

            private final UUID randomUUID = randomUUID();

            @Override
            public Event apply(Event event) {
                if (event instanceof SampleEvent) {
                    final SampleEvent sampleEvent = (SampleEvent) event;
                    if ("BAZ".equals(sampleEvent.getName())) {
                        return new FancyEvent(sampleEvent.getName(), randomUUID);
                    }
                }
                return null;
            }
        }

        final UUID newStreamUuid = unsafeOps.traverseAndReplace(uuid, new StreamChanger());

        final Collection<Event> expected = singletonList(new FancyEvent("BAZ", newStreamUuid));
        assertIterableEquals(expected, store.readBy(newStreamUuid));

        // check that reference from old stream to new stream exists
        final Collection<Event> modifiedSourceEvents = new ArrayList<>(source);
        modifiedSourceEvents.add(new StreamMovedTo(uuid, newStreamUuid));

        assertIterableEquals(modifiedSourceEvents, store.readBy(uuid));
    }

    @Test
    void traverseAndReplaceShouldThrowEventStreamRewriteUnsupportedExceptionWhenUuidsEqual() {
        final SampleEvent event = new SampleEvent("FOO", randomUUID());
        store.write(event);

        assertThrows(EventStreamRewriteUnsupportedException.class,
                () -> unsafeOps.traverseAndReplace(event.uuid(), identity()));

    }

    @Test
    void traverseAndReplaceAllShouldThrowNullPointerOnNullArguments() {
        //noinspection ConstantConditions
        assertThrows(NullPointerException.class, () -> unsafeOps.traverseAndReplaceAll(null, identity()));

        final UUID uuid = randomUUID();
        store.write(new SampleEvent("FOO", uuid));
        //noinspection ConstantConditions
        assertThrows(NullPointerException.class, () -> unsafeOps.traverseAndReplaceAll(uuid, null));
    }

    // empty stream exception will be thrown cause no stream with given uuid exists
    @Test
    void traverseAndReplaceAllShouldThrowEmptyEventStreamExceptionWhenEventsNotFoundByUuid() {
        assertThrows(EmptyEventStreamException.class, () -> unsafeOps.traverseAndReplaceAll(randomUUID(), identity()));
    }

    @Test
    void traverseAndReplaceAllShouldThrowEmptyEventStreamExceptionWhenNoEventsProducedByHandler() {
        final SampleEvent event = new SampleEvent("FOO", randomUUID());
        store.write(event);

        assertThrows(EmptyEventStreamException.class,
                () -> unsafeOps.traverseAndReplaceAll(event.uuid(), collection -> null));
        assertThrows(EmptyEventStreamException.class,
                () -> unsafeOps.traverseAndReplaceAll(event.uuid(), collection -> emptyList()));
    }

    @Test
    void traverseAndReplaceAllShouldThrowEventStreamSplitUnsupportedExceptionWhenMultipleEventsProducedByHandlerWithDifferentUuid() {
        final UUID uuid = randomUUID();
        final Collection<Event> source = asList(new SampleEvent("FOO", uuid), new SampleEvent("BAR", uuid));
        source.forEach(store::write);

        assertThrows(EventStreamSplitUnsupportedException.class, () -> unsafeOps.traverseAndReplaceAll(uuid, events -> {
            Collection<Event> processed = new ArrayList<>();
            for (Event event : events) {
                if (event instanceof SampleEvent) {
                    final SampleEvent sampleEvent = (SampleEvent) event;
                    processed.add(new SampleEvent(sampleEvent.getName(), randomUUID()));
                } else {
                    processed.add(event);
                }
            }
            return processed;
        }));
    }

    @Test
    void traverseAndReplaceAllShouldSuccessfullyChangeStreamContent() {
        // write source events for future processing
        final UUID uuid = randomUUID();
        final Collection<Event> source = asList(
                new SampleEvent("FOO", uuid),
                new SampleEvent("BAR", uuid),
                new SampleEvent("BAZ", uuid)
        );
        source.forEach(store::write);

        // we replace last event from original stream
        class StreamChanger implements UnaryOperator<Collection<Event>> {

            private final UUID randomUUID = randomUUID();

            @Override
            public Collection<Event> apply(Collection<Event> events) {
                Collection<Event> result = new ArrayList<>();
                for (Event event : events) {
                    if (event instanceof SampleEvent) {
                        final SampleEvent sampleEvent = (SampleEvent) event;
                        if ("BAZ".equals(sampleEvent.getName())) {
                            result.add(new FancyEvent(sampleEvent.getName(), randomUUID));
                        } else if ("FOO".equals(sampleEvent.getName())) {
                            result.add(new SampleEvent(sampleEvent.getName(), randomUUID));
                        }
                    }
                }
                return result;
            }
        }

        final UUID newStreamUuid = unsafeOps.traverseAndReplaceAll(uuid, new StreamChanger());

        final Collection<Event> expected = asList(
                new SampleEvent("FOO", newStreamUuid),
                new FancyEvent("BAZ", newStreamUuid)
        );
        assertIterableEquals(expected, store.readBy(newStreamUuid));

        // check that reference from old stream to new stream exists
        final Collection<Event> modifiedSourceEvents = new ArrayList<>(source);
        modifiedSourceEvents.add(new StreamMovedTo(uuid, newStreamUuid));

        assertIterableEquals(modifiedSourceEvents, store.readBy(uuid));
    }

    @Test
    void traverseAndReplaceAllShouldThrowEventStreamRewriteUnsupportedExceptionWhenUuidsEqual() {
        final SampleEvent event = new SampleEvent("FOO", randomUUID());
        store.write(event);

        assertThrows(EventStreamRewriteUnsupportedException.class,
                () -> unsafeOps.traverseAndReplaceAll(event.uuid(), identity()));
    }

}