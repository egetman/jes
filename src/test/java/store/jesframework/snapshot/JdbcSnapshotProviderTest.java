package store.jesframework.snapshot;

import javax.sql.DataSource;

import org.junit.jupiter.api.Test;

import store.jesframework.ex.BrokenStoreException;
import store.jesframework.provider.JdbcStoreProvider;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

class JdbcSnapshotProviderTest {

    @Test
    void exceptionsDuringInitializeShouldBeWrapped() {
        final DataSource dataSource = mock(DataSource.class);
        assertThrows(BrokenStoreException.class, () -> new JdbcStoreProvider<>(dataSource, String.class));
    }

}