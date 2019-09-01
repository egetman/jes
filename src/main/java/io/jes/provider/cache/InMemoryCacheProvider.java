package io.jes.provider.cache;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import io.jes.Event;

public class InMemoryCacheProvider implements CacheProvider {

    private final LocalCache cache;
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    /**
     * Constructor for {@link InMemoryCacheProvider}.
     *
     * @throws IllegalArgumentException if {@literal cacheSize} is less than or equal 0.
     */
    public InMemoryCacheProvider(int cacheSize) {
        if (cacheSize <= 0) {
            throw new IllegalArgumentException("Cache size must be greater than 0: " + cacheSize);
        }
        this.cache = new LocalCache(cacheSize);
    }

    @Nullable
    @Override
    public Event get(long offset) {
        if (offset < 0) {
            throw new IllegalArgumentException("Offset value is less than 0: " + offset);
        }
        try {
            readWriteLock.readLock().lock();
            return cache.get(offset);
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    @Override
    public void put(long offset, @Nonnull Event event) {
        if (offset < 0) {
            throw new IllegalArgumentException("Offset value is less than 0: " + offset);
        }
        try {
            readWriteLock.writeLock().lock();
            cache.put(offset, Objects.requireNonNull(event, "Event must not be null"));
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    @Override
    public void invalidate() {
        try {
            readWriteLock.writeLock().lock();
            cache.clear();
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    private static class LocalCache extends LinkedHashMap<Long, Event> {

        private final int cacheSize;

        LocalCache(int cacheSize) {
            super(cacheSize + 1, 1.0f, true);
            this.cacheSize = cacheSize;
        }

        @Override
        protected boolean removeEldestEntry(Map.Entry<Long, Event> eldest) {
            return super.size() > cacheSize;
        }

        @Override
        public boolean equals(Object o) {
            return super.equals(o) && (o instanceof LocalCache && ((LocalCache) o).cacheSize == cacheSize);
        }

        @Override
        public int hashCode() {
            return super.hashCode() + cacheSize;
        }
    }
}
