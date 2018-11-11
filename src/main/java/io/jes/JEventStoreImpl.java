package io.jes;

import java.util.Collection;
import java.util.UUID;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

import io.jes.ex.EmptyEventStreamException;
import io.jes.provider.StoreProvider;
import io.jes.util.Check;

import static java.util.Objects.requireNonNull;

public class JEventStoreImpl implements JEventStore {

    private final StoreProvider provider;

    public JEventStoreImpl(StoreProvider provider) {
        this.provider = requireNonNull(provider, "StoreProvider must not be null");
    }

    @Override
    public Stream<Event> readFrom(long offset) {
        return provider.readFrom(offset);
    }

    @Override
    public Collection<Event> readBy(@Nonnull UUID uuid) {
        final Collection<Event> events = provider.readBy(requireNonNull(uuid, "Event stream uuid must not be null"));
        Check.nonEmpty(events, () -> new EmptyEventStreamException("Event stream with uuid " + uuid + " not found"));
        return events;
    }

    @Override
    public void write(@Nonnull Event event) {
        provider.write(requireNonNull(event, "Event must not be null"));
    }

    @Override
    public void deleteBy(@Nonnull UUID uuid) {
        provider.deleteBy(requireNonNull(uuid, "Event stream uuid must not be null"));
    }

    @Override
    public void copyTo(@Nonnull JEventStore store) {
        this.copyTo(store, UnaryOperator.identity());
    }

    // todo: to sync or not to sync?
    // need to verify that no events lost after first transfer.
    @Override
    @SuppressWarnings("squid:S1135")
    public void copyTo(@Nonnull JEventStore store, @Nonnull UnaryOperator<Event> handler) {
        requireNonNull(store, "Store must not be null");
        requireNonNull(handler, "Handler must not be null");
        try (final Stream<Event> stream = readFrom(0)) {
            stream.map(handler).forEach(store::write);
        }
    }
}
