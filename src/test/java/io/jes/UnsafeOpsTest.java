package io.jes;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
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
        store = new JEventStoreImpl(new JdbcStoreProvider<>(FancyStuff.newDataSource(), byte[].class));
        unsafeOps = new UnsafeOps(store);
    }

    @Test
    void eventStreamToUniqUuidShouldCorrectlyFilterEvents() {
        final UUID uuid = UUID.randomUUID();
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
    void copyAndReplaceShouldThrowNullPointerOnNullArguments() {
        //noinspection ConstantConditions
        assertThrows(NullPointerException.class, () -> unsafeOps.copyAndReplace(null, identity()));

        final UUID uuid = UUID.randomUUID();
        store.write(new SampleEvent("FOO", uuid));
        //noinspection ConstantConditions
        assertThrows(NullPointerException.class, () -> unsafeOps.copyAndReplace(uuid, null));
    }

    // empty stream exception will be thrown cause no stream with given uuid exists
    @Test
    void copyAndReplaceShouldThrowEmptyEventStreamExceptionWhenEventsNotFoundByUuid() {
        assertThrows(EmptyEventStreamException.class, () -> unsafeOps.copyAndReplace(UUID.randomUUID(), identity()));
    }

    @Test
    void copyAndReplaceShouldThrowEmptyEventStreamExceptionWhenNoEventsProducedByHandler() {
        final SampleEvent event = new SampleEvent("FOO", UUID.randomUUID());
        store.write(event);

        assertThrows(EmptyEventStreamException.class, () -> unsafeOps.copyAndReplace(event.uuid(), obj -> null));
    }

    @Test
    void copyAndReplaceShouldThrowEventStreamSplitUnsupportedExceptionWhenMultipleEventsProducedByHandlerWithDifferentUuid() {
        final UUID uuid = UUID.randomUUID();
        final Collection<Event> source = asList(new SampleEvent("FOO", uuid), new SampleEvent("BAR", uuid));
        source.forEach(store::write);

        assertThrows(EventStreamSplitUnsupportedException.class, () -> unsafeOps.copyAndReplace(uuid, event -> {
            final SampleEvent sampleEvent = (SampleEvent) event;
            return new SampleEvent(sampleEvent.getName(), UUID.randomUUID());
        }));
    }

    @Test
    void copyAndReplaceShouldSuccessfullyChangeStreamContent() {
        // write source events for future processing
        final UUID uuid = UUID.randomUUID();
        final Collection<Event> source = asList(
                new SampleEvent("FOO", uuid),
                new SampleEvent("BAR", uuid),
                new SampleEvent("BAZ", uuid)
        );
        source.forEach(store::write);

        // we replace last event from original stream
        class StreamChanger implements UnaryOperator<Event> {

            private final UUID randomUUID = UUID.randomUUID();

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

        final UUID newStreamUuid = unsafeOps.copyAndReplace(uuid, new StreamChanger());

        final Collection<Event> expected = Collections.singletonList(new FancyEvent("BAZ", newStreamUuid));
        assertIterableEquals(expected, store.readBy(newStreamUuid));

        // check that reference from old stream to new stream exists
        final Collection<Event> modifiedSourceEvents = new ArrayList<>(source);
        modifiedSourceEvents.add(new StreamMovedTo(uuid, newStreamUuid));

        assertIterableEquals(modifiedSourceEvents, store.readBy(uuid));
    }

    @Test
    void copyAndReplaceShouldThrowEventStreamRewriteUnsupportedExceptionWhenUuidsEqual() {
        final SampleEvent event = new SampleEvent("FOO", UUID.randomUUID());
        store.write(event);

        assertThrows(EventStreamRewriteUnsupportedException.class,
                () -> unsafeOps.copyAndReplace(event.uuid(), UnaryOperator.identity()));

    }
}