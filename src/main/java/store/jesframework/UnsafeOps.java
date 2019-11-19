package store.jesframework;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import javax.annotation.Nonnull;

import store.jesframework.common.StreamMergedTo;
import store.jesframework.common.StreamMovedTo;
import store.jesframework.common.StreamSplittedTo;
import store.jesframework.ex.EmptyEventStreamException;
import store.jesframework.ex.EventStreamRewriteUnsupportedException;
import store.jesframework.ex.EventStreamSplitUnsupportedException;
import lombok.extern.slf4j.Slf4j;
import store.jesframework.util.Check;

import static store.jesframework.util.Check.nonEmpty;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

/**
 * This class contains some unsafe methods for manipulating an {@literal Event Store}.
 * Most of that methods should be performed during STW pause, or it can result in an unpredictable state.
 */
@Slf4j
@SuppressWarnings("WeakerAccess")
public class UnsafeOps {

    private static final String APPEND_DEBUG = "Append {} event to event store";
    private static final String NULL_HANDLER_ERROR = "Event handler must not be null";
    private static final String NO_PRODUCED_EVENTS_ERROR = "Produced events must not be null or empty";

    private final JEventStore store;

    @SuppressWarnings("WeakerAccess")
    public UnsafeOps(@Nonnull JEventStore store) {
        this.store = requireNonNull(store, "Event Store must not be null");
    }

    /**
     * Copy the given stream and replace it with the new one in {@literal Event Store} (Old stream will not be deleted).
     * The supplied handler allow change of existing stream.
     * Note: new stream identifier (UUID) must differ from {@literal streamUuid}.
     * I.e. processed event, returned from {@link UnaryOperator#apply(Object)} MUST have new {@link Event#uuid()},
     * and ALL processed events MUST return the same {@link UUID} on {@link Event#uuid()} call.
     * Handler can return null values. Such values will be cleared from the new stream.
     * After replace operation, a new event ({@link StreamMovedTo}) will be appended to source stream.
     * It will contain a reference to the new stream UUID.
     *
     * @param streamUuid uuid of the event stream that will be rewritten.
     * @param handler    an {@link UnaryOperator} to modify found events in stream.
     * @return uuid of the new, rewritten event stream.
     * @throws NullPointerException                   when {@literal streamUuid} is null, or {@literal handler} is null.
     * @throws EmptyEventStreamException              when no event stream found by the given {@literal streamUuid} or
     *                                                if {@literal handler} transform all source events into nulls.
     * @throws EventStreamSplitUnsupportedException   if result event stream return multiple uuids.
     * @throws EventStreamRewriteUnsupportedException if result event stream has the same uuid as {@literal streamUuid}.
     * @see <a href="https://leanpub.com/esversioning/read#leanpub-auto-copy-and-replace">Copy and Replace pattern</a>
     */
    @Nonnull
    public UUID traverseAndReplace(@Nonnull UUID streamUuid, @Nonnull UnaryOperator<Event> handler) {
        requireNonNull(handler, NULL_HANDLER_ERROR);
        final Collection<Event> original = readStream(streamUuid);
        final Collection<Event> replaced = original.stream().map(handler).filter(Objects::nonNull).collect(toList());
        return rewriteStream(streamUuid, replaced);
    }

    /**
     * Copy the given stream and replace it with the new one in {@literal Event Store} (Old stream will not be deleted).
     * The supplied handler allow change of existing stream.
     * Note: new stream identifier (UUID) must differ from {@literal streamUuid}.
     * I.e. each processed event, returned from {@link UnaryOperator#apply(Object)} MUST have new {@link Event#uuid()},
     * and ALL processed events MUST return the same {@link UUID} on {@link Event#uuid()} call.
     * Handler can't return null values.
     * After replace operation, a new event ({@link StreamMovedTo}) will be appended to source stream.
     * It will contain a reference to the new stream UUID.
     *
     * @param streamUuid uuid of the event stream that will be rewritten.
     * @param handler    an {@link UnaryOperator} to modify found events in stream.
     * @return uuid of the new, rewritten event stream.
     * @throws NullPointerException                   when {@literal streamUuid} is null, or {@literal handler} is null.
     * @throws EmptyEventStreamException              when no event stream found by the given {@literal streamUuid} or
     *                                                if {@literal handler} return null or empty collection.
     * @throws EventStreamSplitUnsupportedException   if result event stream return multiple uuids.
     * @throws EventStreamRewriteUnsupportedException if result event stream has the same uuid as {@literal streamUuid}.
     * @see <a href="https://leanpub.com/esversioning/read#leanpub-auto-copy-and-replace">Copy and Replace pattern</a>
     */
    @Nonnull
    public UUID traverseAndReplaceAll(@Nonnull UUID streamUuid, @Nonnull UnaryOperator<Collection<Event>> handler) {
        requireNonNull(handler, NULL_HANDLER_ERROR);
        final Collection<Event> original = readStream(streamUuid);
        final Collection<Event> replaced = handler.apply(original);
        return rewriteStream(streamUuid, replaced);
    }

    /**
     * Copy the given streams and merge them with the new one in {@literal Event Store} (Old streams will not be
     * deleted).
     * The supplied handler allow change of existing streams.
     * Note: new stream identifier (UUID) must differ from any that contains in {@literal streamUuids}.
     * I.e. each processed event, returned from {@link Function#apply(Object)} MUST have new {@link Event#uuid()},
     * and ALL processed events MUST return the same {@link UUID} on {@link Event#uuid()} call.
     * Handler can't return null values.
     * After the merge operation, a new event ({@link StreamMergedTo}) will be appended to all source streams.
     * It will contain a reference to the new stream UUID.
     *
     * @param streamUuids uuids of the event streams that will be merged.
     * @param handler     an {@link Function} to modify found events in stream.
     * @return uuid of the new, merged event stream.
     * @throws NullPointerException                   when {@literal streamUuids} is null, or {@literal handler} is
     *                                                null.
     * @throws EmptyEventStreamException              when no event streams found by the given {@literal streamUuids} or
     *                                                if {@literal handler} return null or empty collection.
     * @throws EventStreamSplitUnsupportedException   if result event stream return multiple uuids.
     * @throws EventStreamRewriteUnsupportedException if result event stream has the same uuid as any of
     *                                                {@literal streamUuids}.
     * @see <a href="https://leanpub.com/esversioning/read#leanpub-auto-copy-and-replace">Copy and Replace pattern</a>
     */
    @Nonnull
    public UUID traverseAndMerge(@Nonnull Set<UUID> streamUuids,
                                 @Nonnull Function<Map<UUID, Collection<Event>>, Collection<Event>> handler) {
        requireNonNull(handler, NULL_HANDLER_ERROR);
        requireNonNull(streamUuids, "Stream uids must not be null");
        final Map<UUID, Collection<Event>> original = readStreams(streamUuids);
        final Collection<Event> replaced = handler.apply(original);
        return mergeStreams(streamUuids, replaced);
    }

    /**
     * Copy the given stream and split it with the multiple ones in {@literal Event Store} (Old streams will not be
     * deleted).
     * The supplied handler allow change of existing stream.
     * Note: new stream identifiers (UUID) must differ from {@literal streamUuid}.
     * I.e. each processed event, returned from {@link Function#apply(Object)} MUST have new {@link Event#uuid()}.
     * Handler can't return null values.
     * After the split operation, a new event ({@link StreamSplittedTo}) will be appended to source stream.
     * It will contain a reference to the new stream UUIDs.
     *
     * @param streamUuid uuid of the event stream that will be splitted.
     * @param handler    an {@link Function} to modify found events in stream.
     * @return uuids of the new, splitted event stream.
     * @throws NullPointerException                   when {@literal streamUuid} is null, or {@literal handler} is
     *                                                null.
     * @throws EmptyEventStreamException              when no event streams found by the given {@literal streamUuid} or
     *                                                if {@literal handler} return null or empty map.
     * @throws EventStreamRewriteUnsupportedException if any of result event streams has the same uuid as the
     *                                                {@literal streamUuid}.
     * @see <a href="https://leanpub.com/esversioning/read#leanpub-auto-copy-and-replace">Copy and Replace pattern</a>
     */
    @Nonnull
    public Set<UUID> traverseAndSplit(@Nonnull UUID streamUuid,
                                      @Nonnull Function<Collection<Event>, Map<UUID, Collection<Event>>> handler) {
        requireNonNull(handler, NULL_HANDLER_ERROR);
        final Collection<Event> original = readStream(streamUuid);
        final Map<UUID, Collection<Event>> replaced = handler.apply(original);
        return splitStream(streamUuid, replaced);
    }

    @Nonnull
    UUID eventStreamToUniqUuid(@Nonnull Collection<? extends Event> events) {
        UUID uuid = null;
        for (Event event : events) {
            final UUID streamUuid = requireNonNull(event.uuid(), "Event uuid must not be null");
            if (uuid == null) {
                uuid = streamUuid;
            } else if (!uuid.equals(streamUuid)) {
                throw new EventStreamSplitUnsupportedException(asList(uuid, streamUuid));
            }
        }
        return Objects.requireNonNull(uuid, "Produced uuid not found");
    }

    // not transactional rewrite. Change it or not?
    @Nonnull
    private UUID rewriteStream(@Nonnull UUID streamUuid, @Nonnull Collection<Event> events) {
        nonEmpty(events, () -> new EmptyEventStreamException(NO_PRODUCED_EVENTS_ERROR));
        final UUID newStreamUuid = eventStreamToUniqUuid(events);
        Check.nonEqual(streamUuid, newStreamUuid, () -> new EventStreamRewriteUnsupportedException(streamUuid));

        events.forEach(store::write);
        final Event moved = new StreamMovedTo(streamUuid, newStreamUuid);
        log.debug(APPEND_DEBUG, moved);
        store.write(moved);
        return newStreamUuid;
    }

    // not transactional rewrite. Change it or not?
    @Nonnull
    private Set<UUID> splitStream(@Nonnull UUID streamUuid, @Nonnull Map<UUID, Collection<Event>> events) {
        nonEmpty(events, () -> new EmptyEventStreamException(NO_PRODUCED_EVENTS_ERROR));
        final Set<UUID> newStreamUuids = events.keySet();

        // need to validate all streams before rewriting. Yes, that is additional collection traverse
        for (UUID newStreamUuid : newStreamUuids) {
            Check.nonEqual(streamUuid, newStreamUuid, () -> new EventStreamRewriteUnsupportedException(streamUuid));
            nonEmpty(events.get(newStreamUuid),
                    () -> new EmptyEventStreamException("No events found for uuid: " + newStreamUuid));
        }

        for (UUID newStreamUuid : newStreamUuids) {
            events.get(newStreamUuid).forEach(store::write);
        }

        final Event splitted = new StreamSplittedTo(streamUuid, newStreamUuids);
        log.debug(APPEND_DEBUG, splitted);
        store.write(splitted);
        return newStreamUuids;
    }

    // not transactional rewrite. Change it or not?
    @Nonnull
    private UUID mergeStreams(@Nonnull Set<UUID> streamUuids, @Nonnull Collection<Event> events) {
        nonEmpty(events, () -> new EmptyEventStreamException(NO_PRODUCED_EVENTS_ERROR));
        final UUID newStreamUuid = eventStreamToUniqUuid(events);
        for (UUID streamUuid : streamUuids) {
            Check.nonEqual(streamUuid, newStreamUuid, () -> new EventStreamRewriteUnsupportedException(streamUuid));
        }
        events.forEach(store::write);
        // second cycle to verify operations consistency
        for (UUID streamUuid : streamUuids) {
            final Event merged = new StreamMergedTo(streamUuid, newStreamUuid);
            log.debug(APPEND_DEBUG, merged);
            store.write(merged);
        }
        return newStreamUuid;
    }

    @Nonnull
    private Collection<Event> readStream(@Nonnull UUID uuid) {
        final Collection<Event> eventStream = store.readBy(uuid);
        nonEmpty(eventStream, () -> new EmptyEventStreamException("Event stream not found by uuid: " + uuid));
        return eventStream;
    }

    @Nonnull
    private Map<UUID, Collection<Event>> readStreams(@Nonnull Set<UUID> uuids) {
        nonEmpty(uuids, () -> new EmptyEventStreamException("Event stream uuids is null or empty"));
        return uuids.stream().map(store::readBy).collect(toMap(events -> {
            // no need to call #eventStreamToUniqUuid. It's events from event store, with equal UUID per collection,
            // so it's ok to check just first
            nonEmpty(events, () -> new EmptyEventStreamException("Event stream not found by one of uuids: " + uuids));
            return events.iterator().next().uuid();
        }, Function.identity()));
    }
}
