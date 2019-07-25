package io.jes.provider.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import lombok.SneakyThrows;

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

        Assertions.assertEquals(PostgresDDL.class,
                DDLFactory.newDDLProducer(newConnectionMock("PostgreSQL", "FOO")).getClass());
    }

    @Test
    void newDDLProducerShouldReturnH2DDLProducerOnCorrectValue() {
        Assertions.assertEquals(H2DDL.class, DDLFactory.newDDLProducer(newConnectionMock("H2", "FOO")).getClass());
    }

    @Test
    void newSnapshotDDLProducerShouldThrowIllegalArgumentExceptionOnUnknownValue() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> DDLFactory.newSnapshotDDLProducer(newConnectionMock("FOO", "FOO")));
    }

    @Test
    void newDDLProducerShouldThrowIllegalArgumentExceptionOnAnyOtherValue() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> DDLFactory.newDDLProducer(newConnectionMock("Oracle DB", "BAR")));

        Assertions.assertThrows(IllegalArgumentException.class,
                () -> DDLFactory.newDDLProducer(newConnectionMock("MySQL", "FOO")));

        Assertions.assertThrows(IllegalArgumentException.class,
                () -> DDLFactory.newDDLProducer(newConnectionMock("DB2", "BAZ")));

    }

}