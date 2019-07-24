package io.jes.reactors;

import java.util.Objects;
import javax.annotation.Nonnull;

import io.jes.JEventStore;
import io.jes.bus.NoopCommandBus;
import io.jes.lock.LockManager;
import io.jes.offset.Offset;

public abstract class Projector extends Reactor {

    private final LockManager lockManager;

    public Projector(@Nonnull JEventStore store, @Nonnull Offset offset, @Nonnull LockManager lockManager) {
        super(store, offset, new NoopCommandBus());
        this.lockManager = Objects.requireNonNull(lockManager, "LockManager must not be null");
    }

    @Override
    void tailStore() {
        lockManager.doProtectedWrite(getKey(), super::tailStore);
    }

    /**
     * Method used to fully recreate projection.
     */
    public void recreate() {
        lockManager.doProtectedWrite(getKey(), () -> {
            offset.reset(getKey());
            onRecreate();
        });
    }

    /**
     * This method used to clean up all state (projection) made by this Projector.
     * Note: this method MUST NOT use any methods that are protected by {@link #lockManager} instance.
     */
    protected abstract void onRecreate();
}
