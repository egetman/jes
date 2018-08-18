package io.jes.provider.jdbc;

import javax.annotation.Nonnull;

public interface DataSourceSyntax {

    @Nonnull
    String createStore(Class<?> contentType);

    @Nonnull
    String eventContentName();

    @Nonnull
    String insertEvents();

    @Nonnull
    String queryEvents();

    @Nonnull
    String queryEventsByStream();

    @Nonnull
    String queryEventsStreamVersion();
}
