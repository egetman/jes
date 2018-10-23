package io.jes.provider;

import javax.annotation.Nonnull;

import lombok.extern.slf4j.Slf4j;

import static io.jes.FancyStuff.newDataSource;

@Slf4j
class JdbcKryoStoreProviderTest extends StoreProviderTest {

    private final StoreProvider provider;

    JdbcKryoStoreProviderTest() {
        this.provider = new JdbcStoreProvider<>(newDataSource(), byte[].class);
    }

    @Nonnull
    @Override
    StoreProvider getProvider() {
        return provider;
    }

}
