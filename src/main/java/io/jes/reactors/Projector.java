package io.jes.reactors;

import java.util.Objects;
import javax.annotation.Nonnull;

import io.jes.JEventStore;
import io.jes.lock.LockManager;
import io.jes.offset.Offset;

@SuppressWarnings("WeakerAccess")
public abstract class Projector extends Reactor {

    private final LockManager lockManager;

    public Projector(@Nonnull JEventStore store, @Nonnull Offset offset, @Nonnull LockManager lockManager) {
        super(store, offset);
        this.lockManager = Objects.requireNonNull(lockManager, "LockManager must no be null");
    }

    @Override
    void tailStore() {
        lockManager.doExclusive(key, super::tailStore);
    }

    public void recreate() {
        lockManager.doExclusive(key, () -> offset.reset(key));
    }

}
