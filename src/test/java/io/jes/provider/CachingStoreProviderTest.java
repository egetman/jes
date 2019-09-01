package io.jes.provider;

import java.util.Collection;
import java.util.UUID;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.redisson.codec.JsonJacksonCodec;

import io.jes.Event;
import io.jes.provider.cache.CaffeineCacheProvider;
import io.jes.provider.cache.InMemoryCacheProvider;
import io.jes.provider.cache.RedisCacheProvider;

import static io.jes.internal.Events.FancyEvent;
import static io.jes.internal.Events.SampleEvent;
import static io.jes.internal.FancyStuff.newRedissonClient;
import static java.util.Arrays.asList;
import static java.util.stream.Stream.empty;
import static java.util.stream.Stream.of;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CachingStoreProviderTest {

    private static StoreProvider mock = mock(StoreProvider.class);

    private static final Collection<StoreProvider> PROVIDERS = asList(
            new CachingStoreProvider(mock, new InMemoryCacheProvider(1)),
            new CachingStoreProvider(mock, new CaffeineCacheProvider(1)),
            new CachingStoreProvider(mock, new RedisCacheProvider(newRedissonClient(), new JsonJacksonCodec(), 1))
    );

    private static Collection<StoreProvider> createProviders() {
        return PROVIDERS;
    }

    @ParameterizedTest
    @MethodSource("createProviders")
    void callShouldBePropagatedToDelegateIfCacheIsEmpty(@Nonnull StoreProvider provider) {
        try (Stream<Event> stream = provider.readFrom(0)) {
            Assertions.assertEquals(0, stream.count());
        }
        verify(mock, times(1)).readFrom(0);
    }

    @ParameterizedTest
    @MethodSource("createProviders")
    void cacheShoultBePopulatedOnRead(@Nonnull StoreProvider provider) {
        when(mock.readFrom(0)).thenReturn(of(new FancyEvent("name", UUID.randomUUID())));

        try (Stream<Event> stream = provider.readFrom(0)) {
            Assertions.assertEquals(1, stream.count());
        }
        verify(mock, times(1)).readFrom(0);

        // second read should be from cache
        try (Stream<Event> stream = provider.readFrom(0)) {
            Assertions.assertEquals(1, stream.count());
        }
        verify(mock, times(1)).readFrom(0);
    }

    @ParameterizedTest
    @MethodSource("createProviders")
    void streamDeletionShouldInvalidateCache(@Nonnull StoreProvider provider) {
        final UUID uuid = UUID.randomUUID();
        when(mock.readFrom(0))
                .thenReturn(of(new FancyEvent("name", uuid)))
                .thenReturn(empty());

        try (Stream<Event> stream = provider.readFrom(0)) {
            Assertions.assertEquals(1, stream.count());
        }
        verify(mock, times(1)).readFrom(0);

        provider.deleteBy(uuid);
        verify(mock, times(1)).deleteBy(uuid);

        // second read should be from delegate
        try (Stream<Event> stream = provider.readFrom(0)) {
            Assertions.assertEquals(0, stream.count());
        }
        verify(mock, times(2)).readFrom(0);
    }

    @ParameterizedTest
    @MethodSource("createProviders")
    void notCachedMethodsShouldBeDelegated(@Nonnull StoreProvider provider) {
        final UUID uuid = UUID.randomUUID();
        provider.readBy(uuid);
        verify(mock, times(1)).readBy(uuid);

        final FancyEvent fancy = new FancyEvent("1", UUID.randomUUID());
        provider.write(fancy);
        verify(mock, times(1)).write(fancy);

        final SampleEvent sample = new SampleEvent("1", UUID.randomUUID());
        provider.write(fancy, sample);
        verify(mock, times(1)).write(fancy, sample);

        provider.deleteBy(uuid);
        verify(mock, times(1)).deleteBy(uuid);
    }

    @AfterEach
    void resetMock() {
        reset(mock);
    }

}
