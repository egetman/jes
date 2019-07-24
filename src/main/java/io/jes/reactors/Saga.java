package io.jes.reactors;

import java.util.Objects;
import javax.annotation.Nonnull;

import io.jes.JEventStore;
import io.jes.bus.CommandBus;
import io.jes.lock.LockManager;
import io.jes.offset.Offset;

@SuppressWarnings("unused")
public class Saga extends Reactor {

    private final LockManager lockManager;

    public Saga(@Nonnull JEventStore store, @Nonnull Offset offset, @Nonnull CommandBus bus,
                @Nonnull LockManager lockManager) {
        super(store, offset, bus);
        this.lockManager = Objects.requireNonNull(lockManager);
    }

    @Override
    void tailStore() {
        lockManager.doProtectedWrite(getKey(), super::tailStore);
    }

}
