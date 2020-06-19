package store.jesframework.provider.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;

import lombok.SneakyThrows;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static store.jesframework.provider.jdbc.DDLFactory.DDLReader.readDDL;
import static store.jesframework.provider.jdbc.DDLFactory.getAggregateStoreDDL;
import static store.jesframework.provider.jdbc.DDLFactory.getEventStoreDDL;
import static store.jesframework.provider.jdbc.DDLFactory.getLockDDL;
import static store.jesframework.provider.jdbc.DDLFactory.getOffsetsDDL;

@Execution(CONCURRENT)
class DDLFactoryTest {

    private static final String H2 = "H2";
    private static final String MY_SQL = "MySQL";
    private static final String POSTGRE_SQL = "PostgreSQL";

    private Connection newConnectionMock(String databaseName, String schema) {
        return newConnectionMock(databaseName, schema, 10);
    }

    @SneakyThrows
    private Connection newConnectionMock(String databaseName, String schema, int version) {
        final Connection connection = mock(Connection.class);
        when(connection.getSchema()).thenReturn(schema);

        final DatabaseMetaData metaData = mock(DatabaseMetaData.class);
        when(metaData.getDatabaseProductName()).thenReturn(databaseName);
        when(metaData.getDatabaseMajorVersion()).thenReturn(version);
        when(connection.getMetaData()).thenReturn(metaData);
        return connection;
    }

    @Test
    void getEventStoreDDLShouldReturnScriptOnCorrectValue() {
        assertNotNull(getEventStoreDDL(newConnectionMock(POSTGRE_SQL, "FOO"), byte[].class));
        assertNotNull(getEventStoreDDL(newConnectionMock(POSTGRE_SQL, "FOO", 9), String.class));
        assertNotNull(getEventStoreDDL(newConnectionMock(H2, "FOO"), String.class));
        assertNotNull(getEventStoreDDL(newConnectionMock(MY_SQL, "FOO"), byte[].class));
        assertNotNull(getEventStoreDDL(newConnectionMock(MY_SQL, "FOO"), String.class));
    }

    @Test
    void getAggregateStoreDDLShouldReturnScriptOnCorrectValue() {
        assertNotNull(getAggregateStoreDDL(newConnectionMock(POSTGRE_SQL, "FOO")));
        assertNotNull(getAggregateStoreDDL(newConnectionMock(POSTGRE_SQL, "FOO", 8)));
        assertNotNull(getAggregateStoreDDL(newConnectionMock(MY_SQL, "FOO")));
    }

    @Test
    void getOffsetDDLShouldReturnScriptOnCorrectValue() {
        assertNotNull(getOffsetsDDL(newConnectionMock(H2, "FOO")));
        assertNotNull(getOffsetsDDL(newConnectionMock(MY_SQL, "FOO")).getClass());
        assertNotNull(getOffsetsDDL(newConnectionMock(POSTGRE_SQL, "FOO")).getClass());
        assertNotNull(getOffsetsDDL(newConnectionMock(POSTGRE_SQL, "FOO", 7)).getClass());
    }

    @Test
    void getAggregateStoreDDLShouldThrowIllegalArgumentExceptionOnUnknownValue() {
        final Connection connection = newConnectionMock("FOO", "FOO");
        assertThrows(IllegalArgumentException.class, () -> getAggregateStoreDDL(connection));
    }

    @Test
    void getEventStoreDDLShouldThrowIllegalArgumentExceptionOnAnyOtherValue() {
        final Connection oraPlusConnection = newConnectionMock("Oracle DB+", "BAR");
        final Connection yourSqlConnection = newConnectionMock("YourSQL", "FOO");
        final Connection db3Connection = newConnectionMock("DB3", "BAZ");
        final Connection h2Connection = newConnectionMock(H2, "BAZ");

        assertThrows(IllegalArgumentException.class, () -> getEventStoreDDL(oraPlusConnection, String.class));
        assertThrows(IllegalArgumentException.class, () -> getEventStoreDDL(yourSqlConnection, byte[].class));
        assertThrows(IllegalArgumentException.class, () -> getEventStoreDDL(db3Connection, String.class));
        assertThrows(IllegalArgumentException.class, () -> getEventStoreDDL(h2Connection, byte.class));
    }

    @Test
    void getOffsetsDDLShouldThrowIllegalArgumentExceptionOnUnknownValue() {
        final Connection connection = newConnectionMock("FOO", "FOO");
        assertThrows(IllegalArgumentException.class, () -> getOffsetsDDL(connection));
    }

    @Test
    void getLockDDLShouldThrowIllegalArgumentExceptionOnUnknownValue() {
        final Connection connection = newConnectionMock("FOO", "FOO");
        assertThrows(IllegalArgumentException.class, () -> getLockDDL(connection));
    }

    @Test
    void getLockDDLShouldReturnScriptOnCorrectValue() {
        assertNotNull(getLockDDL(newConnectionMock(POSTGRE_SQL, "FOO")));
        assertNotNull(getLockDDL(newConnectionMock(POSTGRE_SQL, "FOO", 9)));
        assertNotNull(getLockDDL(newConnectionMock(MY_SQL, "FOO")));
    }

    @Test
    void readNonExistingLocationShouldResultInIllegalStateException() {
        final Exception exception = assertThrows(IllegalStateException.class, () -> readDDL("foo/bar/baz/boo"));
        // verify message contains needed info
        assertEquals("Can't find script: foo/bar/baz/boo", exception.getMessage());
    }

}