package store.jesframework.provider;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.UUID;
import java.util.stream.Stream;
import javax.sql.DataSource;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import lombok.Cleanup;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import store.jesframework.Event;
import store.jesframework.internal.Events.FancyEvent;
import store.jesframework.internal.Events.SampleEvent;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Slf4j
class JdbcClusterStoreProviderTest {

    @Test
    @SneakyThrows
    void sameDataSourceShouldBeUsedIfNoReplicas() {
        final DataSource dataSource = mockDataSource();
        @Cleanup
        final JdbcClusterStoreProvider<String> provider = new JdbcClusterStoreProvider<>(dataSource, null, 1, SECONDS);
        clearInvocations(dataSource);
        //noinspection unused
        @Cleanup
        final Stream<Event> firstStream = provider.readFrom(0);
        // 1 call during schema setup, 1 call during read
        verify(dataSource, times(1)).getConnection();
        clearInvocations(dataSource);

        provider.write(new SampleEvent("Foo"));
        // 1 more call during write
        verify(dataSource, times(1)).getConnection();
        clearInvocations(dataSource);

        //noinspection unused
        @Cleanup
        final Stream<Event> secondStream = provider.readFrom(0);
        // 1 more call during read
        verify(dataSource, times(1)).getConnection();
    }

    @Test
    @SneakyThrows
    void whenReplicaProvidedItShouldHandleReadsByOffset() {
        final DataSource master = mockDataSource();
        final DataSource replica1 = mockDataSource();
        final DataSource replica2 = mockDataSource();
        @Cleanup
        final JdbcClusterStoreProvider<String> provider = new JdbcClusterStoreProvider<>(master,
                asList(replica1, replica2), 1, SECONDS);

        clearInvocations(master);
        provider.write(new SampleEvent("Foo"));

        verify(master, times(1)).getConnection();
        verify(replica1, never()).getConnection();
        verify(replica2, never()).getConnection();
        clearInvocations(master);

        //noinspection unused
        @Cleanup
        final Stream<Event> ignored = provider.readFrom(0);

        verify(master, never()).getConnection();

        int replica1Invocations = Mockito.mockingDetails(replica1).getInvocations().size();
        int replica2Invocations = Mockito.mockingDetails(replica2).getInvocations().size();
        Assertions.assertEquals(1, replica1Invocations + replica2Invocations);
        clearInvocations(replica1, replica2);

        provider.readBy(UUID.randomUUID());
        replica1Invocations = Mockito.mockingDetails(replica1).getInvocations().size();
        replica2Invocations = Mockito.mockingDetails(replica2).getInvocations().size();

        verify(master, never()).getConnection();
        Assertions.assertEquals(1, replica1Invocations + replica2Invocations);
    }

    @Test
    @SneakyThrows
    void readsByWrittenUuidsShouldBeRoutedToMaster() {
        final DataSource master = mockDataSource();
        final DataSource replica = mockDataSource();
        @Cleanup
        final JdbcClusterStoreProvider<String> provider = new JdbcClusterStoreProvider<>(master, replica);
        clearInvocations(master);

        final UUID customUuid = UUID.randomUUID();
        provider.write(new SampleEvent("Baz", customUuid));

        verify(master, times(1)).getConnection();
        verify(replica, never()).getConnection();
        clearInvocations(master);

        provider.readBy(customUuid);
        verify(master, times(1)).getConnection();
        verify(replica, never()).getConnection();
        clearInvocations(master);
    }

    @Test
    @SneakyThrows
    void readsWithSkipByWrittenUuidsShouldBeRoutedToMaster() {
        final DataSource master = mockDataSource();
        final DataSource replica = mockDataSource();
        @Cleanup
        final JdbcClusterStoreProvider<String> provider = new JdbcClusterStoreProvider<>(master, replica);
        clearInvocations(master);

        final UUID customUuid = UUID.randomUUID();
        provider.write(new SampleEvent("Baz", customUuid), new FancyEvent("Bar", customUuid));

        verify(master, times(1)).getConnection();
        verify(replica, never()).getConnection();
        clearInvocations(master);

        provider.readBy(customUuid, 1);
        verify(master, times(1)).getConnection();
        verify(replica, never()).getConnection();
        clearInvocations(master);
    }

    @Test
    @SneakyThrows
    void readsWithSkipByUnseenUuidsShouldBeRoutedToReplica() {
        final DataSource master = mockDataSource();
        final DataSource replica = mockDataSource();
        @Cleanup
        final JdbcClusterStoreProvider<String> provider = new JdbcClusterStoreProvider<>(master, replica);
        clearInvocations(master);

        provider.write(new SampleEvent("Baz"), new SampleEvent("Bar"));

        verify(master, times(1)).getConnection();
        verify(replica, never()).getConnection();
        clearInvocations(master);

        provider.readBy(UUID.randomUUID(), 1);
        verify(master, never()).getConnection();
        verify(replica, times(1)).getConnection();
        clearInvocations(master);
    }

    @Test
    @SneakyThrows
    void readsByDeletedUuidsShouldBeRoutedToMaster() {
        final DataSource master = mockDataSource();
        final DataSource replica = mockDataSource();
        @Cleanup
        final JdbcClusterStoreProvider<String> provider = new JdbcClusterStoreProvider<>(master, replica);
        clearInvocations(master);

        final UUID customUuid = UUID.randomUUID();
        provider.deleteBy(customUuid);

        verify(master, times(1)).getConnection();
        verify(replica, never()).getConnection();
        clearInvocations(master);

        provider.readBy(customUuid);
        verify(master, times(1)).getConnection();
        verify(replica, never()).getConnection();
        clearInvocations(master);
    }

    @Test
    @SneakyThrows
    void readsByWrittenInBatchUuidsShouldBeRoutedToMaster() {
        final DataSource master = mockDataSource();
        final DataSource replica = mockDataSource();
        @Cleanup
        final JdbcClusterStoreProvider<String> provider = new JdbcClusterStoreProvider<>(master, replica);
        clearInvocations(master);

        final UUID customUuid1 = UUID.randomUUID();
        final UUID customUuid2 = UUID.randomUUID();

        provider.write(new SampleEvent("Foo", customUuid1), new FancyEvent("Bar", customUuid2));

        verify(master, times(1)).getConnection();
        verify(replica, never()).getConnection();
        clearInvocations(master);

        provider.readBy(customUuid1);
        verify(master, times(1)).getConnection();
        verify(replica, never()).getConnection();
        clearInvocations(master);

        provider.readBy(customUuid2);
        verify(master, times(1)).getConnection();
        verify(replica, never()).getConnection();
        clearInvocations(master);
    }

    @Test
    @SneakyThrows
    void readsByUuidsShouldBeRoutedToReplicaAfterTheTimeout() {
        final DataSource master = mockDataSource();
        final DataSource replica = mockDataSource();
        @Cleanup
        final JdbcClusterStoreProvider<String> provider = new JdbcClusterStoreProvider<>(master,
                singleton(replica), 1, MILLISECONDS);

        clearInvocations(master);

        final UUID customUuid = UUID.randomUUID();

        provider.write(new SampleEvent("Foo", customUuid));

        verify(master, times(1)).getConnection();
        verify(replica, never()).getConnection();
        clearInvocations(master);

        // just wait for some millis to definitely be sure about cache expiration
        MILLISECONDS.sleep(20);
        provider.readBy(customUuid);
        verify(master, never()).getConnection();
        verify(replica, times(1)).getConnection();
        clearInvocations(replica);
    }

    @SneakyThrows
    private DataSource mockDataSource() {
        final DataSource dataSource = mock(DataSource.class);
        final Connection connection = mock(Connection.class);
        final DatabaseMetaData metaData = mock(DatabaseMetaData.class);
        final PreparedStatement statement = mock(PreparedStatement.class);
        final ResultSet resultSet = mock(ResultSet.class);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.getMetaData()).thenReturn(metaData);

        when(connection.prepareStatement(anyString())).thenReturn(statement);
        //noinspection MagicConstant
        when(connection.prepareStatement(anyString(), anyInt(), anyInt())).thenReturn(statement);

        when(metaData.supportsBatchUpdates()).thenReturn(true);
        when(metaData.getDatabaseProductName()).thenReturn("H2");
        when(statement.executeUpdate()).thenReturn(1);
        when(statement.executeQuery()).thenReturn(resultSet);
        return dataSource;
    }

}