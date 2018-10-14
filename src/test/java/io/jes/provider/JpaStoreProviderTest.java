package io.jes.provider;

import lombok.extern.slf4j.Slf4j;

import static io.jes.provider.FancyStuff.newEntityManager;

@Slf4j
class JpaStoreProviderTest extends StoreProviderTest {

    @Override
    StoreProvider createProvider() {
        return new JpaStoreProvider<>(newEntityManager(), byte[].class);
    }

}