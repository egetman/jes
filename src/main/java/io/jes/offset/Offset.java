package io.jes.offset;

import javax.annotation.Nonnull;

public interface Offset {

    long value(@Nonnull String key);

    void increment(@Nonnull String key);

    void reset(@Nonnull String key);

}
