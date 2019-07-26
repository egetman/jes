package io.jes.reactors;

import java.util.Objects;
import javax.annotation.Nonnull;

import io.jes.JEventStore;
import io.jes.bus.NoopCommandBus;
import io.jes.lock.Lock;
import io.jes.offset.Offset;

public abstract class Projector extends Reactor {

    private final Lock lock;

    public Projector(@Nonnull JEventStore store, @Nonnull Offset offset, @Nonnull Lock lock) {
        super(store, offset, new NoopCommandBus());
        this.lock = Objects.requireNonNull(lock, "LockManager must not be null");
    }

    @Override
    void tailStore() {
        lock.doProtectedWrite(getKey(), super::tailStore);
    }

    /**
     * Method used to fully recreate projection.
     */
    public void recreate() {
        lock.doProtectedWrite(getKey(), () -> {
            offset.reset(getKey());
            onRecreate();
        });
    }

    /**
     * This method used to clean up all state (projection) made by this Projector.
     * Note: this method MUST NOT use any methods that are protected by {@link #lock} instance.
     */
    protected abstract void onRecreate();
}
