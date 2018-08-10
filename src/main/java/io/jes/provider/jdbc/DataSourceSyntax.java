package io.jes.provider.jdbc;

public interface DataSourceSyntax {

    String createStore(Class<?> contentType);

    String writeEvents();

    String readEvents();

    String eventContentName();

    String readEventsByStream();

}
