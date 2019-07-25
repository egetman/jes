package io.jes.util;

import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import lombok.SneakyThrows;

public final class JdbcUtils {

    private JdbcUtils() {
    }

    /**
     * Find out current schema name to use, or default schema name, if not specified.
     *
     * @param connection is an active connection to datasource.
     * @return resolved schema name.
     * @throws NullPointerException if the connection is null.
     */
    @SneakyThrows
    public static String getSchemaName(@Nonnull Connection connection) {
        final String schema = Objects.requireNonNull(connection).getSchema();
        if (schema != null) {
            return schema;
        }
        return PropsReader.getPropety("jes.jdbc.schema-name");
    }

    /**
     * Returns the database name, obtained via connection
     *
     * @param connection is an active connection to datasource.
     * @return the database name, obtained via connection or null, if not present.
     */
    @SneakyThrows
    public static String getDatabaseName(@Nonnull Connection connection) {
        final DatabaseMetaData metaData = Objects.requireNonNull(connection).getMetaData();
        return metaData.getDatabaseProductName();
    }

    /**
     * Unwraps the returned from {@link java.sql.ResultSet#getObject(String)} type to appropriate java type.
     *
     * @param jdbcType type to unwrap.
     * @return unwrapped type from result set.
     * @throws IllegalArgumentException if type is not supported or null.
     */
    @SneakyThrows
    @SuppressWarnings("unchecked")
    public static <T> T unwrapJdbcType(@Nullable Object jdbcType) {
        if (jdbcType instanceof String || jdbcType instanceof byte[]) {
            return (T) jdbcType;
        } else if (jdbcType instanceof Clob) {
            return (T) ((Clob) jdbcType).getSubString(1, (int) ((Clob) jdbcType).length());
        } else if (jdbcType instanceof Blob) {
            return (T) ((Blob) jdbcType).getBytes(1, (int) ((Blob) jdbcType).length());
        }
        throw new IllegalArgumentException("Unsupported jdbc type: " + (jdbcType != null ? jdbcType.getClass() : null));
    }

}
