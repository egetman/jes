package io.jes.provider;

import javax.annotation.Nonnull;

import static io.jes.common.FancyStuff.newEntityManager;

class JpaGsonStoreProviderTest extends StoreProviderTest {

    private final StoreProvider provider;

    JpaGsonStoreProviderTest() {
        this.provider = new JpaStoreProvider<>(newEntityManager(String.class), String.class);
    }

    @Nonnull
    @Override
    StoreProvider getProvider() {
        return provider;
    }

}