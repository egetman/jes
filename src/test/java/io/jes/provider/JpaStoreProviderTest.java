package io.jes.provider;

import javax.annotation.Nonnull;

import lombok.extern.slf4j.Slf4j;

import static io.jes.FancyStuff.newEntityManager;

@Slf4j
class JpaStoreProviderTest extends StoreProviderTest {

    private final StoreProvider provider;

    JpaStoreProviderTest() {
        this.provider = new JpaStoreProvider<>(newEntityManager(), byte[].class);
    }

    @Nonnull
    @Override
    StoreProvider getProvider() {
        return provider;
    }

}