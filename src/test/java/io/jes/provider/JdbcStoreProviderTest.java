package io.jes.provider;

import lombok.extern.slf4j.Slf4j;

import static io.jes.provider.FancyStuff.newDataSource;


@Slf4j
class JdbcStoreProviderTest extends StoreProviderTest {

    @Override
    StoreProvider createProvider() {
        return new JdbcStoreProvider<>(newDataSource(), byte[].class);
    }

}
