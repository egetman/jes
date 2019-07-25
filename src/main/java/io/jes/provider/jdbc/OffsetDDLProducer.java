package io.jes.provider.jdbc;

import javax.annotation.Nonnull;

public interface OffsetDDLProducer {

    /**
     * Creates an offset table.
     *
     * @return ddl statement for offset creation.
     */
    @Nonnull
    String createOffsetTable();

    /**
     * @return SQL statement for offset creation by given key with initial value of 0.
     */
    @Nonnull
    String createOffset();

    /**
     * @return SQL statement for offset incrementation by given key.
     */
    @Nonnull
    String increment();

    /**
     * @return SQL statement for offset current value by given key.
     */
    @Nonnull
    String value();

    /**
     * @return SQL statement for offset reset by given key.
     */
    @Nonnull
    String reset();
}
