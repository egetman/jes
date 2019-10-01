package store.jesframework.reactors;

import javax.annotation.Nonnull;

/**
 * The component that triggers {@literal runnable} on state change or on some another condition.
 */
interface Trigger extends AutoCloseable {

    /**
     * Action to be run on condition met.
     *
     * @param key      is an identifier of owning component.
     * @param runnable is an action to run on state change.
     */
    void onChange(@Nonnull String key, @Nonnull Runnable runnable);

}
