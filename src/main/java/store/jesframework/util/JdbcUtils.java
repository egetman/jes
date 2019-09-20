package store.jesframework.util;

import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.sql.DataSource;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
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
        if (schema != null && !schema.isEmpty()) {
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
     * Returns the database version, obtained via connection
     *
     * @param connection is an active connection to datasource.
     * @return the database majow version, obtained via connection.
     */
    @SneakyThrows
    public static int getDatabaseMajorVersion(@Nonnull Connection connection) {
        final DatabaseMetaData metaData = Objects.requireNonNull(connection).getMetaData();
        return metaData.getDatabaseMajorVersion();
    }

    /**
     * Unwraps the returned from {@link java.sql.ResultSet#getObject(String)} type to appropriate java type.
     *
     * @param jdbcType type to unwrap.
     * @param <T>      is one of appropriate java types.
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

    /**
     * Creates connection with concrete default schema.
     *
     * @param dataSource is {@link DataSource} to produce connections.
     * @return configured connection.
     */
    @SneakyThrows
    public static Connection createConnection(@Nonnull DataSource dataSource) {
        final Connection connection = dataSource.getConnection();
        final String schemaName = getSchemaName(connection);
        if (!schemaName.equals(connection.getSchema())) {
            connection.setSchema(schemaName);
        }
        return connection;
    }

    /**
     * Resolves and return sql type name for specified java type and database vendor name.
     *
     * @param type         is a java type to resolve sql type.
     * @param databaseName is a name of database product.
     * @return SQL type name.
     * @throws NullPointerException     if any of {@code type} or {@code databaseName} is null.
     * @throws IllegalArgumentException if database name is unknown or type is unsupported.
     */
    public static String getSqlTypeByClassAndDatabaseName(@Nonnull Class<?> type, @Nonnull String databaseName) {
        Objects.requireNonNull(type, "Java type to resolve jdbc type must not be null");
        Objects.requireNonNull(databaseName, "Database name must not be null");
        if (type == String.class && ("PostgreSQL".equals(databaseName) || "H2".equals(databaseName))) {
            return "TEXT";
        } else if (type == byte[].class && "PostgreSQL".equals(databaseName)) {
            return "BYTEA";
        } else if (type == byte[].class && "H2".equals(databaseName)) {
            return "BLOB";
        }
        throw new IllegalArgumentException("Unsupported content type: " + type + " for db " + databaseName);
    }

}
