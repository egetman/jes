package io.jes.provider;

import javax.annotation.Nonnull;

import lombok.extern.slf4j.Slf4j;

import static io.jes.common.FancyStuff.newEntityManager;

@Slf4j
class JpaKryoStoreProviderTest extends StoreProviderTest {

    private final StoreProvider provider;

    JpaKryoStoreProviderTest() {
        this.provider = new JpaStoreProvider<>(newEntityManager(byte[].class), byte[].class);
    }

    @Nonnull
    @Override
    StoreProvider getProvider() {
        return provider;
    }

}