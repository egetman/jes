package io.jes.provider.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;

import org.junit.jupiter.api.Test;

import lombok.SneakyThrows;

import static io.jes.provider.jdbc.DDLFactory.getAggregateStoreDDL;
import static io.jes.provider.jdbc.DDLFactory.getEventStoreDDL;
import static io.jes.provider.jdbc.DDLFactory.getLockDDL;
import static io.jes.provider.jdbc.DDLFactory.getOffsetsDDL;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DDLFactoryTest {

    private static final String POSTGRE_SQL = "PostgreSQL";

    @SneakyThrows
    private Connection newConnectionMock(String databaseName, String schema) {
        final Connection connection = mock(Connection.class);
        when(connection.getSchema()).thenReturn(schema);

        final DatabaseMetaData metaData = mock(DatabaseMetaData.class);
        when(metaData.getDatabaseProductName()).thenReturn(databaseName);
        when(connection.getMetaData()).thenReturn(metaData);
        return connection;
    }

    @Test
    void getEventStoreDDLShouldReturnScriptOnCorrectValue() {
        assertNotNull(getEventStoreDDL(newConnectionMock(POSTGRE_SQL, "FOO"), byte[].class));
        assertNotNull(getEventStoreDDL(newConnectionMock("H2", "FOO"), String.class));
    }

    @Test
    void getOffsetDDLShouldReturnStriptOnCorrectValue() {
        assertNotNull(getOffsetsDDL(newConnectionMock("H2", "FOO")));
        assertNotNull(getOffsetsDDL(newConnectionMock(POSTGRE_SQL, "FOO")).getClass());
    }

    @Test
    void getAggregateStoreDDLShouldThrowIllegalArgumentExceptionOnUnknownValue() {
        assertThrows(IllegalArgumentException.class, () -> getAggregateStoreDDL(newConnectionMock("FOO", "FOO")));
    }

    @Test
    void getEventStoreDDLShouldThrowIllegalArgumentExceptionOnAnyOtherValue() {
        assertThrows(IllegalArgumentException.class,
                () -> getEventStoreDDL(newConnectionMock("Oracle DB", "BAR"), String.class));
        assertThrows(IllegalArgumentException.class,
                () -> getEventStoreDDL(newConnectionMock("MySQL", "FOO"), byte[].class));
        assertThrows(IllegalArgumentException.class,
                () -> getEventStoreDDL(newConnectionMock("DB2", "BAZ"), String.class));
        assertThrows(IllegalArgumentException.class,
                () -> getEventStoreDDL(newConnectionMock("H2", "BAZ"), byte.class));
    }

    @Test
    void getOffsetsDDLShouldThrowIllegalArgumentExceptionOnUnknownValue() {
        assertThrows(IllegalArgumentException.class, () -> getOffsetsDDL(newConnectionMock("FOO", "FOO")));
    }

    @Test
    void getLockDDLShouldThrowIllegalArgumentExceptionOnUnknownValue() {
        assertThrows(IllegalArgumentException.class, () -> getLockDDL(newConnectionMock("FOO", "FOO")));
    }

    @Test
    void getLockDDLShouldReturnScriptOnCorrectValue() {
        assertNotNull(getOffsetsDDL(newConnectionMock(POSTGRE_SQL, "FOO")));
    }

}