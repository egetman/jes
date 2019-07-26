package io.jes.provider.jdbc;

import javax.annotation.Nonnull;

public interface LockDDLProducer {

    /**
     * Creates an locks table.
     *
     * @return ddl statement for locks creation.
     */
    @Nonnull
    String createLockTable();

    /**
     * @return SQL statement for acquiring a lock.
     */
    @Nonnull
    String lock();

    /**
     * @return SQL statement for releasing a lock.
     */
    @Nonnull
    String unlock();

}
