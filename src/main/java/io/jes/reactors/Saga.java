package io.jes.reactors;

import java.util.Objects;
import javax.annotation.Nonnull;

import io.jes.JEventStore;
import io.jes.bus.CommandBus;
import io.jes.lock.Lock;
import io.jes.offset.Offset;

/**
 * Note: Sagas are WIP.
 */
public class Saga extends Reactor {

    private final Lock lock;

    public Saga(@Nonnull JEventStore store, @Nonnull Offset offset, @Nonnull CommandBus bus, @Nonnull Lock lock) {
        super(store, offset, bus);
        this.lock = Objects.requireNonNull(lock);
    }

    @Override
    void tailStore() {
        lock.doProtectedWrite(getKey(), super::tailStore);
    }

}
