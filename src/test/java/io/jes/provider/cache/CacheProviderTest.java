package io.jes.provider.cache;

import java.util.Collection;
import java.util.UUID;
import javax.annotation.Nonnull;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.redisson.codec.JsonJacksonCodec;

import static io.jes.internal.Events.FancyEvent;
import static io.jes.internal.FancyStuff.newRedissonClient;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertThrows;

class CacheProviderTest {

    private static final Collection<CacheProvider> PROVIDERS = asList(
            new InMemoryCacheProvider(1),
            new CaffeineCacheProvider(1),
            new RedisCacheProvider(newRedissonClient())
    );

    private static Collection<CacheProvider> cacheProviders() {
        return PROVIDERS;
    }

    @ParameterizedTest
    @MethodSource("cacheProviders")
    @SuppressWarnings("ConstantConditions")
    void cacheProvidersShouldProtectItsInvariants(@Nonnull CacheProvider provider) {
        assertThrows(NullPointerException.class, () -> provider.put(1, null));
        assertThrows(IllegalArgumentException.class, () -> provider.put(-1, new FancyEvent("", UUID.randomUUID())));
        assertThrows(IllegalArgumentException.class, () -> provider.get(-1));
    }

    @Test
    @SuppressWarnings("ConstantConditions")
    void cacheProvidersShouldProtectItsCreationInvariants() {
        assertThrows(NullPointerException.class, () -> new RedisCacheProvider(null));
        assertThrows(NullPointerException.class, () -> new RedisCacheProvider(newRedissonClient(), null, 1));
        assertThrows(IllegalArgumentException.class,
                () -> new RedisCacheProvider(newRedissonClient(), new JsonJacksonCodec(), 0));

        assertThrows(IllegalArgumentException.class, () -> new InMemoryCacheProvider(-1));
        assertThrows(IllegalArgumentException.class, () -> new CaffeineCacheProvider(-1));
    }


}
