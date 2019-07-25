package io.jes.provider.jdbc;

import java.sql.Connection;
import javax.annotation.Nonnull;

import io.jes.util.JdbcUtils;

import static java.lang.String.format;

/**
 * Factory for vendor specific database dialects.
 */
public class DDLFactory {

    private DDLFactory() {
    }

    private static final String UNSUPPORTED_TYPE = "%s for %s type not supported";

    /**
     * Constructs new DDL producer based on DB vendor name and provided schema.
     *
     * @param connection is an active connection to underlying database.
     * @return {@link StoreDDLProducer} for event store.
     */
    public static StoreDDLProducer newDDLProducer(@Nonnull Connection connection) {
        final String databaseName = JdbcUtils.getDatabaseName(connection);
        final String schemaName = JdbcUtils.getSchemaName(connection);
        if ("PostgreSQL".equals(databaseName)) {
            return new PostgresDDL(schemaName);
        } else if ("H2".equals(databaseName)) {
            return new H2DDL(schemaName);
        } else {
            throw new IllegalArgumentException(format(UNSUPPORTED_TYPE, StoreDDLProducer.class, databaseName));
        }
    }

    /**
     * Constructs new DDL producer based on DB vendor name and provided schema.
     *
     * @param connection is an active connection to underlying database.
     * @return {@link SnapshotDDLProducer} for snapshot store.
     */
    public static SnapshotDDLProducer newSnapshotDDLProducer(@Nonnull Connection connection) {
        final String databaseName = JdbcUtils.getDatabaseName(connection);
        final String schemaName = JdbcUtils.getSchemaName(connection);

        if ("PostgreSQL".equals(databaseName)) {
            return new PostgresDDL(schemaName);
        } else {
            throw new IllegalArgumentException(format(UNSUPPORTED_TYPE, SnapshotDDLProducer.class, databaseName));
        }
    }

}
