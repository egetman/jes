package io.jes.provider.jdbc;

import javax.annotation.Nonnull;

import static java.lang.String.format;

/**
 * Factory for vendor specific database dialects.
 */
public class DDLFactory {

    private DDLFactory() {}

    private static final String UNSUPPORTED_TYPE = "%s for %s type not supported";

    /**
     * Constructs new DDL producer based on DB vendor name and provided schema.
     *
     * @param databaseName vendor name of db product. Obtained through
     *                     {@code connection.getMetaData()..getDatabaseProductName()}.
     * @param schema schema name to use.
     * @return {@link StoreDDLProducer} for event store.
     */
    public static StoreDDLProducer newDDLProducer(@Nonnull String databaseName, @Nonnull String schema) {
        if ("PostgreSQL".equals(databaseName)) {
            return new PostgresDDL(schema);
        } else if ("H2".equals(databaseName)) {
           return new H2DDL(schema);
        } else {
            throw new IllegalArgumentException(format(UNSUPPORTED_TYPE, StoreDDLProducer.class, databaseName));
        }
    }

    /**
     * Constructs new DDL producer based on DB vendor name and provided schema.
     *
     * @param databaseName vendor name of db product. Obtained through
     *                     {@code connection.getMetaData()..getDatabaseProductName()}.
     * @param schema schema name to use.
     * @return {@link SnapshotDDLProducer} for snapshot store.
     */
    public static SnapshotDDLProducer newSnapshotDDLProducer(@Nonnull String databaseName, @Nonnull String schema) {
        if ("PostgreSQL".equals(databaseName)) {
            return new PostgresDDL(schema);
        } else {
            throw new IllegalArgumentException(format(UNSUPPORTED_TYPE, SnapshotDDLProducer.class, databaseName));
        }
    }

}
