package io.jes;

import java.util.Collection;
import java.util.Objects;
import java.util.UUID;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

import io.jes.provider.StoreProvider;

@SuppressWarnings({"unused", "WeakerAccess"})
public class JEventStoreImpl implements JEventStore {

    private final StoreProvider provider;

    public JEventStoreImpl(StoreProvider provider) {
        this.provider = Objects.requireNonNull(provider, "StoreProvider must not be null");
    }

    @Override
    public Stream<Event> readFrom(long offset) {
        return provider.readFrom(offset);
    }

    @Override
    public Collection<Event> readBy(@Nonnull UUID uuid) {
        return provider.readBy(Objects.requireNonNull(uuid, "Event stream uuid must not be null"));
    }

    @Override
    public void write(@Nonnull Event event) {
        provider.write(Objects.requireNonNull(event, "Event must not be null"));
    }

    @Override
    public void deleteBy(@Nonnull UUID uuid) {
        provider.deleteBy(Objects.requireNonNull(uuid, "Event stream uuid must not be null"));
    }

    @Override
    public void copyTo(@Nonnull JEventStore store) {
        this.copyTo(store, UnaryOperator.identity());
    }

    @Override
    public void copyTo(@Nonnull JEventStore store, @Nonnull UnaryOperator<Event> handler) {
        Objects.requireNonNull(store, "Store must not be null");
        Objects.requireNonNull(handler, "Handler must not be null");
        try (final Stream<Event> stream = readFrom(0)) {
            stream.map(handler).forEach(store::write);
        }
    }
}
