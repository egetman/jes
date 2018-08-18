package io.jes;

import java.util.Objects;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

import io.jes.provider.StoreProvider;

@SuppressWarnings("unused")
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
    public Stream<Event> readBy(@Nonnull String stream) {
        return provider.readBy(Objects.requireNonNull(stream, "Event stream must not be null"));
    }

    @Override
    public void write(@Nonnull Event event) {
        provider.write(Objects.requireNonNull(event, "Event must not be null"));
    }

}
