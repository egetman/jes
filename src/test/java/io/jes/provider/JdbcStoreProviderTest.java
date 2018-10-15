package io.jes.provider;

import javax.annotation.Nonnull;

import lombok.extern.slf4j.Slf4j;

import static io.jes.provider.FancyStuff.newDataSource;


@Slf4j
class JdbcStoreProviderTest extends StoreProviderTest {

    @Nonnull
    @Override
    StoreProvider getProvider() {
        return new JdbcStoreProvider<>(newDataSource(), byte[].class);
    }

}
