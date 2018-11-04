package io.jes.lock;

import javax.annotation.Nonnull;

public interface LockManager {

    void doExclusive(@Nonnull String key, @Nonnull Runnable action);

    void doProtectedRead(@Nonnull String key, @Nonnull Runnable action);

    void doProtectedWrite(@Nonnull String key, @Nonnull Runnable action);

}
