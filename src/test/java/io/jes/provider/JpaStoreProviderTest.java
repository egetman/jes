package io.jes.provider;

import javax.annotation.Nonnull;

import lombok.extern.slf4j.Slf4j;

import static io.jes.provider.FancyStuff.newEntityManager;

@Slf4j
class JpaStoreProviderTest extends StoreProviderTest {

    @Nonnull
    @Override
    StoreProvider getProvider() {
        return new JpaStoreProvider<>(newEntityManager(), byte[].class);
    }

}