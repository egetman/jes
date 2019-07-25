package io.jes.provider.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;

import org.junit.jupiter.api.Test;

import lombok.SneakyThrows;

import static io.jes.provider.jdbc.DDLFactory.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DDLFactoryTest {

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
    void newDDLProducerShouldReturnPostgreSQLDDLProducerOnCorrectValue() {
        assertEquals(PostgresDDL.class, newDDLProducer(newConnectionMock("PostgreSQL", "FOO")).getClass());
    }

    @Test
    void newDDLProducerShouldReturnH2DDLProducerOnCorrectValue() {
        assertEquals(H2DDL.class, newDDLProducer(newConnectionMock("H2", "FOO")).getClass());
    }

    @Test
    void newDDLOffsetProducerShouldReturnH2DDLProducerOnCorrectValue() {
        assertEquals(H2DDL.class, newOffsetDDLProducer(newConnectionMock("H2", "FOO")).getClass());
    }

    @Test
    void newDDLOffsetProducerShouldReturnPostgresDDLProducerOnCorrectValue() {
        assertEquals(PostgresDDL.class, newOffsetDDLProducer(newConnectionMock("PostgreSQL", "FOO")).getClass());
    }

    @Test
    void newSnapshotDDLProducerShouldThrowIllegalArgumentExceptionOnUnknownValue() {
        assertThrows(IllegalArgumentException.class, () -> newSnapshotDDLProducer(newConnectionMock("FOO", "FOO")));
    }

    @Test
    void newDDLProducerShouldThrowIllegalArgumentExceptionOnAnyOtherValue() {
        assertThrows(IllegalArgumentException.class, () -> newDDLProducer(newConnectionMock("Oracle DB", "BAR")));
        assertThrows(IllegalArgumentException.class, () -> newDDLProducer(newConnectionMock("MySQL", "FOO")));
        assertThrows(IllegalArgumentException.class, () -> newDDLProducer(newConnectionMock("DB2", "BAZ")));
    }

    @Test
    void newOffsetDDLProducerShouldThrowIllegalArgumentExceptionOnUnknownValue() {
        assertThrows(IllegalArgumentException.class, () -> newOffsetDDLProducer(newConnectionMock("FOO", "FOO")));
    }

}