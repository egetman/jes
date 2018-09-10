package io.jes.provider.jdbc;

import javax.annotation.Nonnull;

import static java.lang.String.format;

public class SyntaxFactory {

    private SyntaxFactory() {}

    private static final String UNSUPPORTED_TYPE = "%s for %s type not supported";

    public static DataSourceSyntax newDataSourceSyntax(@Nonnull String databaseName, @Nonnull String schema) {
        if ("PostgreSQL".equals(databaseName)) {
            return new PostgreSQLSyntax(schema);
        } else {
            throw new IllegalArgumentException(format(UNSUPPORTED_TYPE, DataSourceSyntax.class, databaseName));
        }
    }

}
