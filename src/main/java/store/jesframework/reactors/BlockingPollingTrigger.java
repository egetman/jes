package store.jesframework.reactors;

import java.util.Objects;
import javax.annotation.Nonnull;

import store.jesframework.lock.Lock;

class BlockingPollingTrigger extends PollingTrigger {

    private final Lock lock;

    /**
     * Polling trigger that synchronizes among all {@literal key owners} via lock instance.
     *
     * @param lock is a lock to guarantee exclusive access to run action.
     * @throws NullPointerException if {@literal lock} is null.
     */
    BlockingPollingTrigger(@Nonnull Lock lock) {
        this.lock = Objects.requireNonNull(lock, "Lock must not be null");
    }

    @Override
    public void onChange(@Nonnull String key, @Nonnull Runnable runnable) {
        Objects.requireNonNull(runnable, "Runnable action must not be null");
        super.onChange(key, () -> lock.doProtectedWrite(key, runnable));
    }
}
