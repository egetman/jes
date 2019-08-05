package io.jes.offset;

import java.util.Collection;
import java.util.List;
import java.util.UUID;
import javax.annotation.Nonnull;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static io.jes.internal.FancyStuff.newPostgresDataSource;
import static io.jes.internal.FancyStuff.newRedissonClient;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class OffsetTest {

    private static final List<Offset> OFFSETS = asList(
            new InMemoryOffset(),
            new RedissonOffset(newRedissonClient()),
            new JdbcOffset(newPostgresDataSource())
    );

    private static Collection<Offset> createOffsets() {
        return OFFSETS;
    }

    @ParameterizedTest
    @MethodSource("createOffsets")
    @SuppressWarnings("ConstantConditions")
    void shouldThrowNpeOnNullArguments(@Nonnull Offset offset) {
        assertThrows(NullPointerException.class, () -> offset.value(null));
        assertThrows(NullPointerException.class, () -> offset.increment(null));
        assertThrows(NullPointerException.class, () -> offset.reset(null));
    }

    @ParameterizedTest
    @MethodSource("createOffsets")
    void shouldIncrementOffsetValueByKey(@Nonnull Offset offset) {
        final String key = getClass().getName();

        assertEquals(0, offset.value(key));
        offset.increment(key);
        assertEquals(1, offset.value(key));
        offset.increment(key);
        assertEquals(2, offset.value(key));
        assertEquals(0, offset.value(UUID.randomUUID().toString()));
    }

    @ParameterizedTest
    @MethodSource("createOffsets")
    void shouldResetOffsetByKey(@Nonnull Offset offset) {
        final String first = UUID.randomUUID().toString();
        final String second = UUID.randomUUID().toString();

        offset.increment(first);
        offset.increment(first);
        offset.increment(second);

        assertEquals(2, offset.value(first));
        assertEquals(1, offset.value(second));

        offset.reset(first);

        assertEquals(0, offset.value(first));
        assertEquals(1, offset.value(second));
    }
}