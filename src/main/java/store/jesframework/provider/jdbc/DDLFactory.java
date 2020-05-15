package store.jesframework.provider.jdbc;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import javax.annotation.Nonnull;

import lombok.SneakyThrows;
import store.jesframework.util.JdbcUtils;

import static java.lang.String.format;
import static store.jesframework.util.JdbcUtils.getSchemaName;
import static store.jesframework.util.JdbcUtils.getSqlTypeByClassAndDatabaseName;

/**
 * Factory for vendor specific database dialects.
 */
public final class DDLFactory {

    private static final String DB_NAME_H2 = "H2";
    private static final String DB_NAME_MY_SQL = "MySQL";
    private static final String DB_NAME_POSTGRE_SQL = "PostgreSQL";
    private static final int POSTGRESQL_WITH_IDENTITY_VERSION = 10;

    private static final String SCHEMA_NAME_PROPERTY = "schemaName";
    private static final String CONTENT_TYPE_PROPERTY = "contentType";

    private static final String UNSUPPORTED_TYPE = "DDL for %s type not supported";

    private DDLFactory() {
    }

    /**
     * Constructs new DDL producer based on DB vendor name and provided schema.
     *
     * @param connection is an active connection to underlying database.
     * @param type       is a type of payload to use. Currently, one of {@literal String} or {@literal byte[]}
     * @return ddl for event store.
     */
    @SneakyThrows
    public static String getEventStoreDDL(@Nonnull Connection connection, @Nonnull Class<?> type) {
        final String databaseName = JdbcUtils.getDatabaseName(connection);
        final String schemaName = getSchemaName(connection);
        final String contentType = getSqlTypeByClassAndDatabaseName(type, databaseName);

        switch (databaseName) {
            case DB_NAME_POSTGRE_SQL:
                final int postgreSqlVersion = JdbcUtils.getDatabaseMajorVersion(connection);
                if (postgreSqlVersion >= POSTGRESQL_WITH_IDENTITY_VERSION) {
                    return replaceWith(readDDL("ddl/postgresql/v10/event-store-postgres.ddl"), schemaName, contentType);
                } else {
                    return replaceWith(readDDL("ddl/postgresql/v9/event-store-postgres.ddl"), schemaName, contentType);
                }
            case DB_NAME_H2:
                return replaceWith(readDDL("ddl/h2/event-store-h2.ddl"), schemaName, contentType);
            case DB_NAME_MY_SQL:
                return replaceWith(readDDL("ddl/mysql/event-store-mysql.ddl"), schemaName, contentType);
            default:
                throw new IllegalArgumentException(format(UNSUPPORTED_TYPE, databaseName));
        }
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
            final int postgreSqlVersion = JdbcUtils.getDatabaseMajorVersion(connection);
            if (postgreSqlVersion >= POSTGRESQL_WITH_IDENTITY_VERSION) {
                return replaceWith(readDDL("ddl/postgresql/v10/snapshot-store-postgres.ddl"), schemaName);
            } else {
                return replaceWith(readDDL("ddl/postgresql/v9/snapshot-store-postgres.ddl"), schemaName);
            }
        }
        throw new IllegalArgumentException(format(UNSUPPORTED_TYPE, databaseName));
    }

    /**
     * Constructs new DDL producer based on DB vendor name and provided schema.
     *
     * @param connection is an active connection to underlying database.
     * @return ddl for offset.
     */
    public static String getOffsetsDDL(@Nonnull Connection connection) {
        final String databaseName = JdbcUtils.getDatabaseName(connection);
        final String schemaName = getSchemaName(connection);

        switch (databaseName) {
            case DB_NAME_POSTGRE_SQL:
                final int postgreSqlVersion = JdbcUtils.getDatabaseMajorVersion(connection);
                if (postgreSqlVersion >= POSTGRESQL_WITH_IDENTITY_VERSION) {
                    return replaceWith(readDDL("ddl/postgresql/v10/offsets-postgres.ddl"), schemaName);
                } else {
                    return replaceWith(readDDL("ddl/postgresql/v9/offsets-postgres.ddl"), schemaName);
                }
            case DB_NAME_H2:
                return replaceWith(readDDL("ddl/h2/offsets-h2.ddl"), schemaName);
            case DB_NAME_MY_SQL:
                return replaceWith(readDDL("ddl/mysql/offsets-mysql.ddl"), schemaName);
            default:
                throw new IllegalArgumentException(format(UNSUPPORTED_TYPE, databaseName));
        }
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
            final int postgreSqlVersion = JdbcUtils.getDatabaseMajorVersion(connection);
            if (postgreSqlVersion >= POSTGRESQL_WITH_IDENTITY_VERSION) {
                return replaceWith(readDDL("ddl/postgresql/v10/locks-postgres.ddl"), schemaName);
            } else {
                return replaceWith(readDDL("ddl/postgresql/v9/locks-postgres.ddl"), schemaName);
            }
        } else if (DB_NAME_MY_SQL.equals(databaseName)) {
            return replaceWith(readDDL("ddl/mysql/locks-mysql.ddl"), schemaName);
        }
        throw new IllegalArgumentException(format(UNSUPPORTED_TYPE, databaseName));
    }

    @SneakyThrows
    static String readDDL(@Nonnull String location) {
        final ClassLoader classLoader = DDLFactory.class.getClassLoader();
        try (final InputStream inputStream = classLoader.getResourceAsStream(location)) {
            if (inputStream != null) {
                final byte[] content = new byte[inputStream.available()];
                final int read = inputStream.read(content);
                if (read != content.length) {
                    throw new IllegalStateException("Can't read ddl: " + location);
                }
                return new String(content, StandardCharsets.UTF_8);
            }
        }
        throw new IllegalStateException("Can't find script: " + location);
    }

    @Nonnull
    private static String replaceWith(@Nonnull String script, @Nonnull String schemaName) {
        return script.replace(SCHEMA_NAME_PROPERTY, schemaName);
    }

    @Nonnull
    private static String replaceWith(@Nonnull String script, @Nonnull String schemaName, @Nonnull String contentType) {
        return script.replace(SCHEMA_NAME_PROPERTY, schemaName).replace(CONTENT_TYPE_PROPERTY, contentType);
    }

}
