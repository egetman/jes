package io.jes;

import java.util.Objects;
import javax.annotation.Nonnull;

import io.jes.lock.LockManager;

@SuppressWarnings("unused")
public abstract class Projector extends Reactor {

    private final LockManager lockManager;

    public Projector(@Nonnull JEventStore store, @Nonnull LockManager lockManager) {
        super(store);
        this.lockManager = Objects.requireNonNull(lockManager, "Coordinator must no be null");
    }

    @Override
    void tailStore() {
        lockManager.doExclusive(getClass().getName(), super::tailStore);
    }

    @SuppressWarnings("unused")
    public void recreate() {
        lockManager.doExclusive(getClass().getName(), offset::reset);
    }

}
