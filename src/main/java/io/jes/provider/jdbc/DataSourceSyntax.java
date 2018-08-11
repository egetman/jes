package io.jes.provider.jdbc;

import javax.annotation.Nonnull;

public interface DataSourceSyntax {

    @Nonnull
    String createStore(Class<?> contentType);

    @Nonnull
    String eventContentName();

    @Nonnull
    String writeEvents();

    @Nonnull
    String readEvents();

    @Nonnull
    String readEventsByStream();

    @Nonnull
    String eventsStreamVersion();
}
