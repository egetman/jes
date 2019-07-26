package io.jes.lock;

import javax.annotation.Nonnull;

/**
 * Lock manager controls {@link io.jes.reactors.Projector}/{@literal Sagas} {@link io.jes.JEventStore} tailing in
 * clustered environment. (To avoid situations when multiple services read the same offset from
 * {@link io.jes.JEventStore} and apply duplicate changes concurrently).
 *
 * <p>Note: there no any reentrancy guarantee by contract. Different implementations may or may not behave as reentrant.
 */
public interface Lock {

    /**
     * Executes {@code action} in exclusive write mode, all other clients wait until this action completes.
     *
     * @param key    key to obtain lock instance.
     * @param action action to perform.
     */
    void doProtectedWrite(@Nonnull String key, @Nonnull Runnable action);

}
