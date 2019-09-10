package store.jesframework.lock;

import javax.annotation.Nonnull;

import store.jesframework.JEventStore;
import store.jesframework.reactors.Projector;

/**
 * Lock manager controls {@link Projector}/{@literal Sagas} {@link JEventStore} tailing in
 * clustered environment. (To avoid situations when multiple services read the same offset from
 * {@link JEventStore} and apply duplicate changes concurrently).
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
