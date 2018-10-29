package io.jes;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import io.jes.common.FancyEvent;
import io.jes.common.SampleEvent;
import io.jes.common.StreamMergedTo;
import io.jes.common.StreamMovedTo;
import io.jes.common.StreamSplittedTo;
import io.jes.ex.EmptyEventStreamException;
import io.jes.ex.EventStreamRewriteUnsupportedException;
import io.jes.ex.EventStreamSplitUnsupportedException;
import io.jes.provider.JdbcStoreProvider;

import static io.jes.common.FancyStuff.newDataSource;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.UUID.randomUUID;
import static java.util.function.UnaryOperator.identity;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
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
        assertThrows(NullPointerException.class, () -> unsafeOps.eventStreamToUniqUuid(emptyList()));

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

    @Test
    void traverseAndSplitShouldThrowNullPointerOnNullArguments() {
        //noinspection ConstantConditions
        assertThrows(NullPointerException.class, () -> unsafeOps.traverseAndSplit(null, collection -> emptyMap()));

        final UUID uuid = randomUUID();
        store.write(new SampleEvent("FOO", uuid));
        //noinspection ConstantConditions
        assertThrows(NullPointerException.class, () -> unsafeOps.traverseAndSplit(uuid, null));
    }

    @Test
    void traverseAndMergeShouldThrowNullPointerOnNullArguments() {
        //noinspection ConstantConditions
        assertThrows(NullPointerException.class, () -> unsafeOps.traverseAndMerge(null, map -> emptyList()));

        final UUID uuid = randomUUID();
        store.write(new SampleEvent("FOO", uuid));
        //noinspection ConstantConditions
        assertThrows(NullPointerException.class, () -> unsafeOps.traverseAndMerge(singleton(uuid), null));
    }

    // empty stream exception will be thrown cause no stream with given uuid exists
    @Test
    void traverseAndReplaceShouldThrowEmptyEventStreamExceptionWhenEventsNotFoundByUuid() {
        assertThrows(EmptyEventStreamException.class, () -> unsafeOps.traverseAndReplace(randomUUID(), identity()));
    }

    @Test
    void traverseAndSplitShouldThrowEmptyEventStreamExceptionWhenEventsNotFoundByUuid() {
        assertThrows(EmptyEventStreamException.class, () -> unsafeOps.traverseAndSplit(randomUUID(), colection -> emptyMap()));
    }

    @Test
    void traverseAndMergeShouldThrowEmptyEventStreamExceptionWhenEventsNotFoundByUuid() {
        assertThrows(EmptyEventStreamException.class, () -> unsafeOps.traverseAndMerge(emptySet(), map -> emptyList()));
    }

    @Test
    void traverseAndReplaceShouldThrowEmptyEventStreamExceptionWhenNoEventsProducedByHandler() {
        final SampleEvent event = new SampleEvent("FOO", randomUUID());
        store.write(event);

        assertThrows(EmptyEventStreamException.class, () -> unsafeOps.traverseAndReplace(event.uuid(), obj -> null));
    }

    @Test
    void traverseAndSplitShouldThrowEmptyEventStreamExceptionWhenNoEventsProducedByHandler() {
        final SampleEvent event = new SampleEvent("FOO", randomUUID());
        store.write(event);

        assertThrows(EmptyEventStreamException.class, () -> unsafeOps.traverseAndSplit(event.uuid(), colection -> emptyMap()));
    }

    @Test
    void traverseAndMergeShouldThrowEmptyEventStreamExceptionWhenNoEventsProducedByHandler() {
        final Event fancy = new FancyEvent("BAR", UUID.randomUUID());
        final Event sample = new SampleEvent("FOO", randomUUID());
        store.write(fancy);
        store.write(sample);

        final Set<UUID> uuids = new HashSet<>();
        uuids.add(fancy.uuid());
        uuids.add(sample.uuid());

        assertThrows(EmptyEventStreamException.class, () -> unsafeOps.traverseAndMerge(uuids, map -> emptyList()));
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

    @Test
    void traverseAndSplitShouldProduceTwoStreamsFromOne() {
        // write source events for future processing
        final UUID uuid = randomUUID();
        final Collection<Event> source = asList(
                new SampleEvent("FOO_1", uuid),
                new SampleEvent("FOO_2", uuid),
                new SampleEvent("FOO_3", uuid)
        );
        source.forEach(store::write);

        // we replace last event from original stream
        class StreamChanger implements Function<Collection<Event>, Map<UUID, Collection<Event>>> {

            @Override
            public Map<UUID, Collection<Event>> apply(Collection<Event> events) {
                // let's devide events by 2
                Map<UUID, Collection<Event>> result = new HashMap<>();
                UUID generated = UUID.randomUUID();
                for (Event event : events) {
                    result.putIfAbsent(generated, new ArrayList<>());
                    if (result.get(generated).size() < 2) {
                        result.get(generated).add(new SampleEvent(((SampleEvent) event).getName(), generated));
                    } else {
                        generated = UUID.randomUUID();
                        result.put(generated, new ArrayList<>());
                        result.get(generated).add(new SampleEvent(((SampleEvent) event).getName(), generated));
                    }
                }
                return result;
            }
        }

        final Set<UUID> newStreamUuids = unsafeOps.traverseAndSplit(uuid, new StreamChanger());
        assertEquals(2, newStreamUuids.size(), "Produced streams count don't match");

        for (UUID newStreamUuid : newStreamUuids) {
            // we don't know what uuid we processing, cause set unordered
            final Collection<Event> read = store.readBy(newStreamUuid);
            if (read.size() == 1) {
                // ok, it's second stream
                assertIterableEquals(singletonList(new SampleEvent("FOO_3", newStreamUuid)), read);
            } else {
                assertIterableEquals(
                        asList(new SampleEvent("FOO_1", newStreamUuid), new SampleEvent("FOO_2", newStreamUuid)),
                        read
                );
            }
        }

        // check that reference from old stream to new stream exists
        final Collection<Event> modifiedSourceEvents = new ArrayList<>(source);
        modifiedSourceEvents.add(new StreamSplittedTo(uuid, newStreamUuids));

        assertIterableEquals(modifiedSourceEvents, store.readBy(uuid));
    }

    @Test
    void traverseAndMergeShouldMergeTwoStreamsInToOne() {

        final UUID fancyUuid = UUID.randomUUID();
        final Event fancy = new FancyEvent("FANCY", fancyUuid);

        final UUID sampleUuid = UUID.randomUUID();
        final Event sample = new SampleEvent("SAMPLE", sampleUuid);

        store.write(fancy);
        store.write(sample);

        final Set<UUID> uuids = new HashSet<>(asList(fancy.uuid(), sample.uuid()));

        // we replace last event from original stream
        class StreamChanger implements Function<Map<UUID, Collection<Event>>, Collection<Event>> {

            private final UUID uuid = UUID.randomUUID();

            @Override
            public Collection<Event> apply(Map<UUID, Collection<Event>> events) {
                return events.values().stream().flatMap(Collection::stream).map(event -> {
                    if (event instanceof SampleEvent) {
                        return new SampleEvent(((SampleEvent) event).getName(), uuid);
                    } else if (event instanceof FancyEvent) {
                        return new FancyEvent(((FancyEvent) event).getName(), uuid);
                    }
                    return event;
                }).collect(Collectors.toList());
            }
        }

        final UUID newStreamUuid = unsafeOps.traverseAndMerge(uuids, new StreamChanger());
        final Collection<Event> expected = asList(
                new FancyEvent("FANCY", newStreamUuid),
                new SampleEvent("SAMPLE", newStreamUuid)
        );

        final ArrayList<Event> actual = new ArrayList<>(store.readBy(newStreamUuid));
        actual.sort(Comparator.comparing(Event::uuid));
        assertIterableEquals(expected, actual);

        // check that reference from old stream to new stream exists
        final Collection<Event> expectedFirstSourceStream = asList(fancy, new StreamMergedTo(fancyUuid, newStreamUuid));
        assertIterableEquals(expectedFirstSourceStream, store.readBy(fancyUuid));

        final Collection<Event> expectedSecondSourceStream = asList(sample, new StreamMergedTo(sampleUuid, newStreamUuid));
        assertIterableEquals(expectedSecondSourceStream, store.readBy(sampleUuid));
    }

}