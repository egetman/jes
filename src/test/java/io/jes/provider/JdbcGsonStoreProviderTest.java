package io.jes.provider;

import javax.annotation.Nonnull;

import lombok.extern.slf4j.Slf4j;

import static io.jes.FancyStuff.newDataSource;


@Slf4j
class JdbcGsonStoreProviderTest extends StoreProviderTest {

    private final StoreProvider provider;

    JdbcGsonStoreProviderTest() {
        this.provider = new JdbcStoreProvider<>(newDataSource(), String.class);
    }

    @Nonnull
    @Override
    StoreProvider getProvider() {
        return provider;
    }

}
