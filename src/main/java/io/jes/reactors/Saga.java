package io.jes.reactors;

import javax.annotation.Nonnull;

import io.jes.JEventStore;
import io.jes.bus.CommandBus;
import io.jes.offset.Offset;

public class Saga extends Reactor {

    public Saga(@Nonnull JEventStore store, @Nonnull Offset offset, @Nonnull CommandBus bus) {
        super(store, offset, bus);
    }
}
