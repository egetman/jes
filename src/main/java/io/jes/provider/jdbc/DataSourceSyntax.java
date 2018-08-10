package io.jes.provider.jdbc;

public interface DataSourceSyntax {

    String createStore();

    String writeEvents();

    String readEvents();

    String eventContentName();

    String readEventsByStream();

    String sequenceValueName();

    String nextSequenceNumber();
}
