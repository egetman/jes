package io.jes.provider;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

import io.jes.Event;
import io.jes.provider.cache.CacheProvider;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CachingStoreProvider implements StoreProvider, AutoCloseable {

    private final CacheProvider cache;
    private final StoreProvider delegate;

    @SuppressWarnings("WeakerAccess")
    public CachingStoreProvider(@Nonnull StoreProvider delegate, @Nonnull CacheProvider cache) {
        this.cache = Objects.requireNonNull(cache, "Cache must not be null");
        this.delegate = Objects.requireNonNull(delegate, "StoreProvider must not be null");
    }

    @Override
    @SuppressWarnings("squid:S3864")
    public Stream<Event> readFrom(long offset) {
        final List<Event> cached = new ArrayList<>();
        Event probe;
        while ((probe = cache.get(offset)) != null) {
            cached.add(probe);
            offset++;
        }
        final LongAdder counter = new LongAdder();
        counter.add(offset);
        return Stream.concat(cached.stream(), delegate.readFrom(offset).peek(event -> {
            cache.put(counter.longValue(), event);
            counter.increment();
        }));
    }

    @Override
    public Collection<Event> readBy(@Nonnull UUID uuid) {
        return delegate.readBy(uuid);
    }

    @Override
    public void write(@Nonnull Event event) {
        delegate.write(event);
    }

    @Override
    public void write(@Nonnull Event... events) {
        delegate.write(events);
    }

    @Override
    public void deleteBy(@Nonnull UUID uuid) {
        cache.invalidate();
        delegate.deleteBy(uuid);
    }

    @Override
    @SneakyThrows
    public void close() {
        if (delegate instanceof AutoCloseable) {
            ((AutoCloseable) delegate).close();
        }
        if (cache instanceof AutoCloseable) {
            ((AutoCloseable) cache).close();
        }
    }
}
