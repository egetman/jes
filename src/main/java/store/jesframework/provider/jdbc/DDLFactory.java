package store.jesframework.provider.jdbc;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import lombok.SneakyThrows;
import store.jesframework.util.JdbcUtils;

import static store.jesframework.util.JdbcUtils.getSchemaName;
import static store.jesframework.util.JdbcUtils.getSqlTypeByClassAndDatabaseName;

/**
 * Factory for vendor specific database dialects.
 */
@SuppressWarnings("squid:S1135")
public final class DDLFactory {

    private static final String DB_NAME_H2 = "H2";
    private static final String DB_NAME_MY_SQL = "MySQL";
    private static final String DB_NAME_POSTGRE_SQL = "PostgreSQL";
    private static final int POSTGRESQL_WITH_IDENTITY_VERSION = 10;

    private static final String SCHEMA_NAME_PROPERTY = "schemaName";
    private static final String CONTENT_TYPE_PROPERTY = "contentType";

    // only reads
    private static final Map<String, DDLReader> LOCK_DDLS = new HashMap<>();
    private static final Map<String, DDLReader> OFFSET_DDLS = new HashMap<>();
    private static final Map<String, DDLReader> EVENT_STORE_DDLS = new HashMap<>();
    private static final Map<String, DDLReader> AGGREGATE_STORE_DDLS = new HashMap<>();

    // init on load
    static {
        LOCK_DDLS.put(DB_NAME_MY_SQL, new DDLReader("ddl/mysql/locks-mysql.ddl"));
        LOCK_DDLS.put(DB_NAME_POSTGRE_SQL, new DDLReader(version -> {
            if (version >= POSTGRESQL_WITH_IDENTITY_VERSION) {
                return "ddl/postgresql/v10/locks-postgres.ddl";
            }
            return "ddl/postgresql/v9/locks-postgres.ddl";
        }));

        OFFSET_DDLS.put(DB_NAME_H2, new DDLReader("ddl/h2/offsets-h2.ddl"));
        OFFSET_DDLS.put(DB_NAME_MY_SQL, new DDLReader("ddl/mysql/offsets-mysql.ddl"));
        OFFSET_DDLS.put(DB_NAME_POSTGRE_SQL, new DDLReader(version -> {
            if (version >= POSTGRESQL_WITH_IDENTITY_VERSION) {
                return "ddl/postgresql/v10/offsets-postgres.ddl";
            }
            return "ddl/postgresql/v9/offsets-postgres.ddl";
        }));

        EVENT_STORE_DDLS.put(DB_NAME_H2, new DDLReader("ddl/h2/event-store-h2.ddl"));
        EVENT_STORE_DDLS.put(DB_NAME_MY_SQL, new DDLReader("ddl/mysql/event-store-mysql.ddl"));
        EVENT_STORE_DDLS.put(DB_NAME_POSTGRE_SQL, new DDLReader(version -> {
            if (version >= POSTGRESQL_WITH_IDENTITY_VERSION) {
                return "ddl/postgresql/v10/event-store-postgres.ddl";
            }
            return "ddl/postgresql/v9/event-store-postgres.ddl";
        }));

        AGGREGATE_STORE_DDLS.put(DB_NAME_MY_SQL, new DDLReader("ddl/mysql/snapshot-store-mysql.ddl"));
        AGGREGATE_STORE_DDLS.put(DB_NAME_POSTGRE_SQL, new DDLReader(version -> {
            if (version >= POSTGRESQL_WITH_IDENTITY_VERSION) {
                return "ddl/postgresql/v10/snapshot-store-postgres.ddl";
            }
            return "ddl/postgresql/v9/snapshot-store-postgres.ddl";
        }));
    }

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
        final String contentType = getSqlTypeByClassAndDatabaseName(type, databaseName);
        return getDDL(connection, EVENT_STORE_DDLS.get(databaseName), contentType);
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
        return getDDL(connection, AGGREGATE_STORE_DDLS.get(databaseName), null);
    }

    /**
     * Constructs new DDL producer based on DB vendor name and provided schema.
     *
     * @param connection is an active connection to underlying database.
     * @return ddl for offset.
     */
    public static String getOffsetsDDL(@Nonnull Connection connection) {
        final String databaseName = JdbcUtils.getDatabaseName(connection);
        return getDDL(connection, OFFSET_DDLS.get(databaseName), null);
    }

    /**
     * Constructs new DDL lock producer based on DB vendor name and provided schema.
     *
     * @param connection is an active connection to underlying database.
     * @return ddl for offset.
     */
    public static String getLockDDL(@Nonnull Connection connection) {
        final String databaseName = JdbcUtils.getDatabaseName(connection);
        return getDDL(connection, LOCK_DDLS.get(databaseName), null);
    }

    private static String getDDL(@Nonnull Connection connection, @Nullable DDLReader reader, @Nullable String content) {
        if (reader == null) {
            // query database name one more time just for exception message
            final String databaseName = JdbcUtils.getDatabaseName(connection);
            throw new IllegalArgumentException(String.format("DDL for %s type not supported", databaseName));
        }

        final String schemaName = getSchemaName(connection);
        final int databaseVersion = JdbcUtils.getDatabaseMajorVersion(connection);
        return reader.read(schemaName, content, databaseVersion);
    }

    static class DDLReader {

        private final Function<Integer, String> locationByVersion;

        private DDLReader(@Nonnull String location) {
            this(n -> location);
        }

        private DDLReader(@Nonnull Function<Integer, String> locationByVersion) {
            this.locationByVersion = Objects.requireNonNull(locationByVersion);
        }

        String read(@Nonnull String schemaName, int version) {
            return replaceWith(readDDL(locationByVersion.apply(version)), schemaName);
        }

        String read(@Nonnull String schemaName, @Nullable String contentType, int version) {
            if (contentType == null) {
                return read(schemaName, version);
            }
            return replaceWith(readDDL(locationByVersion.apply(version)), schemaName, contentType);
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
        private static String replaceWith(@Nonnull String script, @Nonnull String schemaName,
                                          @Nonnull String contentType) {
            return script.replace(SCHEMA_NAME_PROPERTY, schemaName).replace(CONTENT_TYPE_PROPERTY, contentType);
        }

    }
}
