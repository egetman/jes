package store.jesframework;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import store.jesframework.common.StreamMergedTo;
import store.jesframework.common.StreamMovedTo;
import store.jesframework.common.StreamSplittedTo;
import store.jesframework.ex.EmptyEventStreamException;
import store.jesframework.ex.EventStreamRewriteUnsupportedException;
import store.jesframework.ex.EventStreamSplitUnsupportedException;
import store.jesframework.internal.Events;
import store.jesframework.provider.JdbcStoreProvider;
import store.jesframework.serializer.api.Format;

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
import static store.jesframework.internal.FancyStuff.newPostgresDataSource;

@TestInstance(Lifecycle.PER_CLASS)
class UnsafeOpsTest {

    private final JEventStore store;
    private final UnsafeOps unsafeOps;

    UnsafeOpsTest() {
        store = new JEventStore(new JdbcStoreProvider<>(newPostgresDataSource(), Format.BINARY_KRYO));
        unsafeOps = new UnsafeOps(store);
    }

    @Test
    void eventStreamToUniqUuidShouldCorrectlyFilterEvents() {
        final UUID uuid = randomUUID();
        assertThrows(NullPointerException.class, () -> unsafeOps.eventStreamToUniqUuid(emptyList()));

        assertEquals(uuid, unsafeOps.eventStreamToUniqUuid(singletonList(new Events.SampleEvent("", uuid))));
        assertEquals(uuid, unsafeOps.eventStreamToUniqUuid(asList(
                new Events.SampleEvent("FOO", uuid),
                new Events.SampleEvent("BAR", uuid)
        )));

        assertThrows(EventStreamSplitUnsupportedException.class, () -> unsafeOps.eventStreamToUniqUuid(asList(
                new Events.SampleEvent("FOO"),
                new Events.SampleEvent("BAR")
        )));
    }

    @Test
    void traverseAndReplaceShouldThrowNullPointerOnNullArguments() {
        //noinspection ConstantConditions
        assertThrows(NullPointerException.class, () -> unsafeOps.traverseAndReplace(null, identity()));

        final UUID uuid = randomUUID();
        store.write(new Events.SampleEvent("FOO", uuid));
        //noinspection ConstantConditions
        assertThrows(NullPointerException.class, () -> unsafeOps.traverseAndReplace(uuid, null));
    }

    @Test
    void traverseAndSplitShouldThrowNullPointerOnNullArguments() {
        //noinspection ConstantConditions
        assertThrows(NullPointerException.class, () -> unsafeOps.traverseAndSplit(null, collection -> emptyMap()));

        final UUID uuid = randomUUID();
        store.write(new Events.SampleEvent("FOO", uuid));
        //noinspection ConstantConditions
        assertThrows(NullPointerException.class, () -> unsafeOps.traverseAndSplit(uuid, null));
    }

    @Test
    void traverseAndMergeShouldThrowNullPointerOnNullArguments() {
        //noinspection ConstantConditions
        assertThrows(NullPointerException.class, () -> unsafeOps.traverseAndMerge(null, map -> emptyList()));

        final UUID uuid = randomUUID();
        store.write(new Events.SampleEvent("FOO", uuid));
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
        final Events.SampleEvent event = new Events.SampleEvent("FOO", randomUUID());
        store.write(event);

        assertThrows(EmptyEventStreamException.class, () -> unsafeOps.traverseAndReplace(event.uuid(), obj -> null));
    }

    @Test
    void traverseAndSplitShouldThrowEmptyEventStreamExceptionWhenNoEventsProducedByHandler() {
        final UUID uuid = randomUUID();
        final Events.SampleEvent event = new Events.SampleEvent("FOO", uuid);
        store.write(event);

        Map<UUID, Collection<Event>> fakeResults = new HashMap<>();
        fakeResults.put(randomUUID(), emptyList());

        assertThrows(EmptyEventStreamException.class, () -> unsafeOps.traverseAndSplit(uuid, colection -> emptyMap()));
        assertThrows(EmptyEventStreamException.class, () -> unsafeOps.traverseAndSplit(uuid, collection -> fakeResults));
    }

    @Test
    void traverseAndMergeShouldThrowEmptyEventStreamExceptionWhenNoEventsProducedByHandler() {
        final Event fancy = new Events.FancyEvent("BAR", UUID.randomUUID());
        final Event sample = new Events.SampleEvent("FOO", randomUUID());
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
        final Collection<Event> source = asList(new Events.SampleEvent("FOO", uuid), new Events.SampleEvent("BAR", uuid));
        source.forEach(store::write);

        assertThrows(EventStreamSplitUnsupportedException.class, () -> unsafeOps.traverseAndReplace(uuid, event -> {
            final Events.SampleEvent sampleEvent = (Events.SampleEvent) event;
            return new Events.SampleEvent(sampleEvent.getName(), randomUUID());
        }));
    }

    @Test
    void traverseAndReplaceShouldSuccessfullyChangeStreamContent() {
        // write source events for future processing
        final UUID uuid = randomUUID();
        final Collection<Event> source = asList(
                new Events.SampleEvent("FOO", uuid),
                new Events.SampleEvent("BAR", uuid),
                new Events.SampleEvent("BAZ", uuid)
        );
        source.forEach(store::write);

        // we replace last event from original stream
        class StreamChanger implements UnaryOperator<Event> {

            private final UUID randomUUID = randomUUID();

            @Override
            public Event apply(Event event) {
                if (event instanceof Events.SampleEvent) {
                    final Events.SampleEvent sampleEvent = (Events.SampleEvent) event;
                    if ("BAZ".equals(sampleEvent.getName())) {
                        return new Events.FancyEvent(sampleEvent.getName(), randomUUID);
                    }
                }
                return null;
            }
        }

        final UUID newStreamUuid = unsafeOps.traverseAndReplace(uuid, new StreamChanger());

        final Collection<Event> expected = singletonList(new Events.FancyEvent("BAZ", newStreamUuid));
        assertIterableEquals(expected, store.readBy(newStreamUuid));

        // check reference from the old stream to the new stream exists
        final Collection<Event> modifiedSourceEvents = new ArrayList<>(source);
        modifiedSourceEvents.add(new StreamMovedTo(uuid, newStreamUuid));

        assertIterableEquals(modifiedSourceEvents, store.readBy(uuid));
    }

    @Test
    void traverseAndReplaceShouldThrowEventStreamRewriteUnsupportedExceptionWhenUuidsEqual() {
        final Events.SampleEvent event = new Events.SampleEvent("FOO", randomUUID());
        store.write(event);

        assertThrows(EventStreamRewriteUnsupportedException.class,
                () -> unsafeOps.traverseAndReplace(event.uuid(), identity()));

    }

    @Test
    void traverseAndReplaceAllShouldThrowNullPointerOnNullArguments() {
        //noinspection ConstantConditions
        assertThrows(NullPointerException.class, () -> unsafeOps.traverseAndReplaceAll(null, identity()));

        final UUID uuid = randomUUID();
        store.write(new Events.SampleEvent("FOO", uuid));
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
        final Events.SampleEvent event = new Events.SampleEvent("FOO", randomUUID());
        store.write(event);

        assertThrows(EmptyEventStreamException.class,
                () -> unsafeOps.traverseAndReplaceAll(event.uuid(), collection -> null));
        assertThrows(EmptyEventStreamException.class,
                () -> unsafeOps.traverseAndReplaceAll(event.uuid(), collection -> emptyList()));
    }

    @Test
    void traverseAndReplaceAllShouldThrowEventStreamSplitUnsupportedExceptionWhenMultipleEventsProducedByHandlerWithDifferentUuid() {
        final UUID uuid = randomUUID();
        final Collection<Event> source = asList(new Events.SampleEvent("FOO", uuid), new Events.SampleEvent("BAR", uuid));
        source.forEach(store::write);

        assertThrows(EventStreamSplitUnsupportedException.class, () -> unsafeOps.traverseAndReplaceAll(uuid, events -> {
            Collection<Event> processed = new ArrayList<>();
            for (Event event : events) {
                if (event instanceof Events.SampleEvent) {
                    final Events.SampleEvent sampleEvent = (Events.SampleEvent) event;
                    processed.add(new Events.SampleEvent(sampleEvent.getName(), randomUUID()));
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
                new Events.SampleEvent("FOO", uuid),
                new Events.SampleEvent("BAR", uuid),
                new Events.SampleEvent("BAZ", uuid)
        );
        source.forEach(store::write);

        // we replace last event from original stream
        class StreamChanger implements UnaryOperator<Collection<Event>> {

            private final UUID randomUUID = randomUUID();

            @Override
            public Collection<Event> apply(Collection<Event> events) {
                Collection<Event> result = new ArrayList<>();
                for (Event event : events) {
                    if (event instanceof Events.SampleEvent) {
                        final Events.SampleEvent sampleEvent = (Events.SampleEvent) event;
                        if ("BAZ".equals(sampleEvent.getName())) {
                            result.add(new Events.FancyEvent(sampleEvent.getName(), randomUUID));
                        } else if ("FOO".equals(sampleEvent.getName())) {
                            result.add(new Events.SampleEvent(sampleEvent.getName(), randomUUID));
                        }
                    }
                }
                return result;
            }
        }

        final UUID newStreamUuid = unsafeOps.traverseAndReplaceAll(uuid, new StreamChanger());

        final Collection<Event> expected = asList(
                new Events.SampleEvent("FOO", newStreamUuid),
                new Events.FancyEvent("BAZ", newStreamUuid)
        );
        assertIterableEquals(expected, store.readBy(newStreamUuid));

        // check reference from the old stream to the new stream exists
        final Collection<Event> modifiedSourceEvents = new ArrayList<>(source);
        modifiedSourceEvents.add(new StreamMovedTo(uuid, newStreamUuid));

        assertIterableEquals(modifiedSourceEvents, store.readBy(uuid));
    }

    @Test
    void traverseAndReplaceAllShouldThrowEventStreamRewriteUnsupportedExceptionWhenUuidsEqual() {
        final Events.SampleEvent event = new Events.SampleEvent("FOO", randomUUID());
        store.write(event);

        assertThrows(EventStreamRewriteUnsupportedException.class,
                () -> unsafeOps.traverseAndReplaceAll(event.uuid(), identity()));
    }

    @Test
    void traverseAndSplitShouldProduceTwoStreamsFromOne() {
        // write source events for future processing
        final UUID uuid = randomUUID();
        final Collection<Event> source = asList(
                new Events.SampleEvent("FOO_1", uuid),
                new Events.SampleEvent("FOO_2", uuid),
                new Events.SampleEvent("FOO_3", uuid)
        );
        source.forEach(store::write);

        // we replace last event from original stream
        class StreamChanger implements Function<Collection<Event>, Map<UUID, Collection<Event>>> {

            @Override
            public Map<UUID, Collection<Event>> apply(Collection<Event> events) {
                // lets devide events by 2
                Map<UUID, Collection<Event>> result = new HashMap<>();
                UUID generated = UUID.randomUUID();
                for (Event event : events) {
                    result.putIfAbsent(generated, new ArrayList<>());
                    if (result.get(generated).size() < 2) {
                        result.get(generated).add(new Events.SampleEvent(((Events.SampleEvent) event).getName(), generated));
                    } else {
                        generated = UUID.randomUUID();
                        result.put(generated, new ArrayList<>());
                        result.get(generated).add(new Events.SampleEvent(((Events.SampleEvent) event).getName(), generated));
                    }
                }
                return result;
            }
        }

        final Set<UUID> newStreamUuids = unsafeOps.traverseAndSplit(uuid, new StreamChanger());
        assertEquals(2, newStreamUuids.size(), "Produced streams count don't match");

        for (UUID newStreamUuid : newStreamUuids) {
            // we don't know what uuid we process, cause set unordered
            final Collection<Event> read = store.readBy(newStreamUuid);
            if (read.size() == 1) {
                // ok, it's second stream
                assertIterableEquals(singletonList(new Events.SampleEvent("FOO_3", newStreamUuid)), read);
            } else {
                assertIterableEquals(
                        asList(new Events.SampleEvent("FOO_1", newStreamUuid), new Events.SampleEvent("FOO_2", newStreamUuid)),
                        read
                );
            }
        }

        // check reference from the old stream to the new stream exists
        final Collection<Event> modifiedSourceEvents = new ArrayList<>(source);
        modifiedSourceEvents.add(new StreamSplittedTo(uuid, newStreamUuids));

        assertIterableEquals(modifiedSourceEvents, store.readBy(uuid));
    }

    @Test
    void traverseAndMergeShouldMergeTwoStreamsInToOne() {

        final UUID fancyUuid = UUID.randomUUID();
        final Event fancy = new Events.FancyEvent("FANCY", fancyUuid);

        final UUID sampleUuid = UUID.randomUUID();
        final Event sample = new Events.SampleEvent("SAMPLE", sampleUuid);

        store.write(fancy);
        store.write(sample);

        final Set<UUID> uuids = new HashSet<>(asList(fancy.uuid(), sample.uuid()));

        // we replace last event from original stream
        class StreamChanger implements Function<Map<UUID, Collection<Event>>, Collection<Event>> {

            private final UUID uuid = UUID.randomUUID();

            @Override
            public Collection<Event> apply(Map<UUID, Collection<Event>> events) {
                return events.values().stream().flatMap(Collection::stream).map(event -> {
                    if (event instanceof Events.SampleEvent) {
                        return new Events.SampleEvent(((Events.SampleEvent) event).getName(), uuid);
                    } else if (event instanceof Events.FancyEvent) {
                        return new Events.FancyEvent(((Events.FancyEvent) event).getName(), uuid);
                    }
                    return event;
                }).collect(Collectors.toList());
            }
        }

        final UUID newStreamUuid = unsafeOps.traverseAndMerge(uuids, new StreamChanger());

        final Comparator<Event> comparator = Comparator.comparing(Event::hashCode);
        final List<Event> expected = asList(
                new Events.FancyEvent("FANCY", newStreamUuid),
                new Events.SampleEvent("SAMPLE", newStreamUuid)
        );
        final List<Event> actual = new ArrayList<>(store.readBy(newStreamUuid));
        // "read" order not specified, so if u pass n uuid's there is no garantee, which will be read first
        actual.sort(comparator);
        expected.sort(comparator);

        assertIterableEquals(expected, actual);

        // check reference from the old stream to the new stream exists
        final Collection<Event> expectedFirstSourceStream = asList(fancy, new StreamMergedTo(fancyUuid, newStreamUuid));
        assertIterableEquals(expectedFirstSourceStream, store.readBy(fancyUuid));

        final Collection<Event> expectedSecondSourceStream = asList(sample, new StreamMergedTo(sampleUuid, newStreamUuid));
        assertIterableEquals(expectedSecondSourceStream, store.readBy(sampleUuid));
    }

}