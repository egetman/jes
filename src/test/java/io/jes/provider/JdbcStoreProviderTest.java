package io.jes.provider;

import lombok.extern.slf4j.Slf4j;

import static io.jes.provider.InfrastructureFactory.newDataSource;

// todo: version caching? to avoid every-write check
// todo: multithreaded write - to be or not to be?
// todo: store drain-to? - recreate event store
// todo: event-intersepter ?? handling?
// todo: make snapshotting
// todo: store structure validation on start
// todo: event idempotency on read (clustered environment)


@Slf4j
class JdbcStoreProviderTest extends StoreProviderTest {

    @Override
    StoreProvider createProvider() {
        return new JdbcStoreProvider<>(newDataSource(), byte[].class);
    }

}
