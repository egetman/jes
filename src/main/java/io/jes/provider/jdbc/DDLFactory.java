package io.jes.provider.jdbc;

import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import javax.annotation.Nonnull;

import io.jes.util.JdbcUtils;
import lombok.SneakyThrows;

import static io.jes.util.JdbcUtils.getSchemaName;
import static io.jes.util.JdbcUtils.getSqlTypeByClassAndDatabaseName;
import static java.lang.String.format;
import static java.nio.file.Files.readAllBytes;
import static java.nio.file.Paths.get;

/**
 * Factory for vendor specific database dialects.
 */
public class DDLFactory {

    private static final String DB_NAME_H2 = "H2";
    private static final String DB_NAME_POSTGRE_SQL = "PostgreSQL";

    private static final String SCHEMA_NAME_PROPERTY = "schemaName";

    private static final String UNSUPPORTED_TYPE = "DDL for %s type not supported";

    private DDLFactory() {
    }

    /**
     * Constructs new DDL producer based on DB vendor name and provided schema.
     *
     * @param connection is an active connection to underlying database.
     * @param type       is a type of payload to use. Currently one of {@literal String} or {@literal byte[]}
     * @return ddl for event store.
     */
    @SneakyThrows
    public static String getEventStoreDDL(@Nonnull Connection connection, @Nonnull Class<?> type) {
        final String databaseName = JdbcUtils.getDatabaseName(connection);
        final String contentType = getSqlTypeByClassAndDatabaseName(type, databaseName);
        final String script;

        if (DB_NAME_POSTGRE_SQL.equals(databaseName)) {
            script = readDDL("ddl/event-store-postgres.ddl");
        } else if (DB_NAME_H2.equals(databaseName)) {
            script = readDDL("ddl/event-store-h2.ddl");
        } else {
            throw new IllegalArgumentException(format(UNSUPPORTED_TYPE, databaseName));
        }
        final String schemaName = getSchemaName(connection);
        return script.replaceAll(SCHEMA_NAME_PROPERTY, schemaName).replaceAll("contentType", contentType);
    }

    /**
     * Constructs new DDL producer based on DB vendor name and provided schema.
     *
     * @param connection is an active connection to underlying database.
     * @return ddl for snapshot store.
     */
    @SneakyThrows
    public static String getAggregateStoreDDL(@Nonnull Connection connection) {
        final String databaseName = JdbcUtils.getDatabaseName(connection);
        final String schemaName = getSchemaName(connection);

        if (DB_NAME_POSTGRE_SQL.equals(databaseName)) {
            final String script = readDDL("ddl/snapshot-store-postgres.ddl");
            return script.replaceAll(SCHEMA_NAME_PROPERTY, schemaName);
        } else {
            throw new IllegalArgumentException(format(UNSUPPORTED_TYPE, databaseName));
        }
    }

    /**
     * Constructs new DDL producer based on DB vendor name and provided schema.
     *
     * @param connection is an active connection to underlying database.
     * @return ddl for offset.
     */
    public static String getOffsetsDDL(@Nonnull Connection connection) {
        final String databaseName = JdbcUtils.getDatabaseName(connection);

        final String script;
        if (DB_NAME_POSTGRE_SQL.equals(databaseName)) {
            script = readDDL("ddl/offsets-postgres.ddl");
        } else if (DB_NAME_H2.equals(databaseName)) {
            script = readDDL("ddl/offsets-h2.ddl");
        } else {
            throw new IllegalArgumentException(format(UNSUPPORTED_TYPE, databaseName));
        }

        final String schemaName = getSchemaName(connection);
        return script.replaceAll(SCHEMA_NAME_PROPERTY, schemaName);
    }

    /**
     * Constructs new DDL lock producer based on DB vendor name and provided schema.
     *
     * @param connection is an active connection to underlying database.
     * @return ddl for offset.
     */
    public static String getLockDDL(@Nonnull Connection connection) {
        final String databaseName = JdbcUtils.getDatabaseName(connection);
        final String schemaName = getSchemaName(connection);
        if (DB_NAME_POSTGRE_SQL.equals(databaseName)) {
            final String script = readDDL("ddl/locks-postgres.ddl");
            return script.replaceAll(SCHEMA_NAME_PROPERTY, schemaName);
        } else {
            throw new IllegalArgumentException(format(UNSUPPORTED_TYPE, databaseName));
        }
    }

    @SneakyThrows
    private static String readDDL(@Nonnull String location) {
        final ClassLoader classLoader = DDLFactory.class.getClassLoader();
        final URL resource = classLoader.getResource(location);
        if (resource != null) {
            return new String(readAllBytes(get(resource.toURI())), StandardCharsets.UTF_8);
        }
        throw new IllegalStateException("Can't find script: " + location);
    }
}
