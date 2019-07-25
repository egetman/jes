package io.jes.util;

import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;

import org.junit.jupiter.api.Test;

import lombok.SneakyThrows;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class JdbcUtilsTest {

    @Test
    void getSchemaShouldThrowNullPoinerExceptionIfConnectionIsNull() {
        //noinspection ConstantConditions
        assertThrows(NullPointerException.class, () -> JdbcUtils.getSchemaName(null));
    }

    @Test
    @SneakyThrows
    void getSchemaShouldReturnSchemaNameFromConnection() {
        final Connection connection = mock(Connection.class);
        when(connection.getSchema()).thenReturn("foo");
        assertEquals("foo", JdbcUtils.getSchemaName(connection));
        verify(connection, times(1)).getSchema();
    }

    @Test
    @SneakyThrows
    void getSchemaShouldReturnSchemaNameFromPropertiesIfConnectionReturnsNull() {
        final Connection connection = mock(Connection.class);
        assertEquals("es", JdbcUtils.getSchemaName(connection));
        verify(connection, times(1)).getSchema();
    }

    @Test
    void getDatabaseNameShouldThrowNullPoinerExceptionIfConnectionIsNull() {
        //noinspection ConstantConditions
        assertThrows(NullPointerException.class, () -> JdbcUtils.getDatabaseName(null));
    }

    @Test
    @SneakyThrows
    void getSchemaShouldReturnDatabaseNameFromConnection() {
        final Connection connection = mock(Connection.class);
        final DatabaseMetaData metaData = mock(DatabaseMetaData.class);
        when(connection.getMetaData()).thenReturn(metaData);
        when(metaData.getDatabaseProductName()).thenReturn("product");
        assertEquals("product", JdbcUtils.getDatabaseName(connection));
        verify(connection, times(1)).getMetaData();
        verify(metaData, times(1)).getDatabaseProductName();
    }

    @Test
    void unwrapJdbcTypeShouldThrowIllegalArgumentExceptionIfTypeIsNullOrUnsupported() {
        assertThrows(IllegalArgumentException.class, () -> JdbcUtils.unwrapJdbcType(null));
        assertThrows(IllegalArgumentException.class, () -> JdbcUtils.unwrapJdbcType(new Object()));
    }

    @Test
    void unwrapJdbcTypeShouldResolveRegisteredTypes() {
        assertDoesNotThrow(() -> JdbcUtils.unwrapJdbcType("Foo"));
        assertDoesNotThrow(() -> JdbcUtils.unwrapJdbcType(new byte[] {}));
        assertDoesNotThrow(() -> JdbcUtils.unwrapJdbcType(mock(Clob.class)));
        assertDoesNotThrow(() -> JdbcUtils.unwrapJdbcType(mock(Blob.class)));
    }

}