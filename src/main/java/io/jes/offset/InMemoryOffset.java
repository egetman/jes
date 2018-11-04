package io.jes.offset;


import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import javax.annotation.Nonnull;

public class InMemoryOffset implements Offset {

    private static final String MISSING_KEY = "Key must not be null";
    private static final Map<String, LongAdder> OFFSETS = new ConcurrentHashMap<>();

    @Override
    public long value(@Nonnull String key) {
        return getOffsetByKey(key).longValue();
    }

    @Override
    public void increment(@Nonnull String key) {
        getOffsetByKey(key).increment();
    }

    @Override
    public void reset(@Nonnull String key) {
        getOffsetByKey(key).reset();
    }

    @Nonnull
    private LongAdder getOffsetByKey(@Nonnull String key) {
        return OFFSETS.computeIfAbsent(Objects.requireNonNull(key, MISSING_KEY), ignored -> new LongAdder());
    }
}
