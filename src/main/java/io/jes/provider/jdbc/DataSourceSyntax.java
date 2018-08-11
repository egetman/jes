package io.jes.provider.jdbc;

public interface DataSourceSyntax {

    String createStore(Class<?> contentType);

    String eventContentName();


    String writeEvents();

    String readEvents();

    String readEventsByStream();

    String eventsStreamVersion();
}
