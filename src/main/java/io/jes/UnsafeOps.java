package io.jes;

import java.util.Collection;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import javax.annotation.Nonnull;

import io.jes.common.StreamMovedTo;
import io.jes.ex.EmptyEventStreamException;
import io.jes.ex.EventStreamRewriteUnsupportedException;
import io.jes.ex.EventStreamSplitUnsupportedException;
import lombok.extern.slf4j.Slf4j;

import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

/**
 * This class contains some unsafe methods for manipulating an {@literal Event Store}.
 * Most of that methods should be performed during STW pause, or it can result in an unpredictable state.
 */
@Slf4j
@SuppressWarnings("WeakerAccess")
public class UnsafeOps {

    private final JEventStore store;

    @SuppressWarnings("WeakerAccess")
    public UnsafeOps(@Nonnull JEventStore store) {
        this.store = requireNonNull(store, "Event Store must not be null");
    }

    /**
     * Copy the given stream and replace it with the new one in {@literal Event Store}.
     * Supplied handler allow change of existing stream.
     * Note: new stream identifier (UUID) must differ from {@literal streamUuid}.
     * I.e. processed event, returned from {@link UnaryOperator#apply(Object)} MUST have new {@link Event#uuid()},
     * and ALL processed events MUST return the same {@link UUID} on {@link Event#uuid()} call.
     * Handler can return null values. Such values will be cleared from the new stream.
     * After the replace operation, a new event ({@link StreamMovedTo}) will be appended to source stream.
     * It will contain a reference to new stream UUID.
     *
     * @param streamUuid uuid of the event stream that will be rewritten.
     * @param handler    an {@link UnaryOperator} to modify found events in stream.
     * @return uuid of new, rewritten event stream.
     * @throws NullPointerException                   when {@literal streamUuid} is null, or {@literal handler} is null.
     * @throws EmptyEventStreamException              when no event stream found by the given {@literal streamUuid} or
     *                                                if {@literal handler} transform all source events into nulls.
     * @throws EventStreamSplitUnsupportedException   if result event stream return multiple uuids.
     * @throws EventStreamRewriteUnsupportedException if result event stream has the same uuid as {@literal streamUuid}.
     * @see <a href="https://leanpub.com/esversioning/read#leanpub-auto-copy-and-replace">Copy and Replace pattern</a>.
     */
    public UUID traverseAndReplace(@Nonnull UUID streamUuid, @Nonnull UnaryOperator<Event> handler) {
        requireNonNull(handler, "Event handler must not be null");

        final Collection<Event> original = store.readBy(streamUuid);
        nonEmpty(original, () -> new EmptyEventStreamException("Event stream not found by uuid: " + streamUuid));

        final Collection<Event> replaced = original.stream().map(handler).filter(Objects::nonNull).collect(toList());
        nonEmpty(replaced, () -> new EmptyEventStreamException("Result stream must not be empty"));

        return rewriteStream(streamUuid, replaced);
    }

    public UUID traverseAndReplaceAll(@Nonnull UUID streamUuid, @Nonnull UnaryOperator<Collection<Event>> handler) {
        requireNonNull(handler, "Event handler must not be null");

        final Collection<Event> original = store.readBy(streamUuid);
        nonEmpty(original, () -> new EmptyEventStreamException("Event stream not found by uuid: " + streamUuid));

        final Collection<? extends Event> replaced = handler.apply(original);
        nonEmpty(replaced, () -> new EmptyEventStreamException("Result stream must not be empty"));

        return rewriteStream(streamUuid, replaced);
    }

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
        return uuid;
    }

    private UUID rewriteStream(@Nonnull UUID streamUuid, @Nonnull Collection<? extends Event> events) {
        final UUID newStreamUuid = eventStreamToUniqUuid(events);
        nonEqual(streamUuid, newStreamUuid, () -> new EventStreamRewriteUnsupportedException(streamUuid));

        events.forEach(store::write);
        final Event moved = new StreamMovedTo(streamUuid, newStreamUuid);
        log.debug("Append {} to Event Store", moved);
        store.write(moved);
        return newStreamUuid;
    }

    private void nonEmpty(Collection<?> events, @Nonnull Supplier<? extends RuntimeException> supplier) {
        if (events == null || events.isEmpty()) {
            throw supplier.get();
        }
    }

    private void nonEqual(@Nonnull Object left, @Nonnull Object right,
                          @Nonnull Supplier<? extends RuntimeException> supplier) {
        if (Objects.equals(left, right)) {
            throw supplier.get();
        }
    }
}
