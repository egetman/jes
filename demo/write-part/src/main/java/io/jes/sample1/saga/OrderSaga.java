package io.jes.sample1.saga;

import javax.annotation.Nonnull;

import org.springframework.stereotype.Component;

import io.jes.AggregateStore;
import io.jes.JEventStore;
import io.jes.bus.CommandBus;
import io.jes.lock.LockManager;
import io.jes.offset.Offset;
import io.jes.reactors.ReactsOn;
import io.jes.reactors.Saga;
import io.jes.sample1.command.RemoveFromStock;
import io.jes.sample1.domain.Item;
import io.jes.sample1.event.OrderPlaced;

@Component
public class OrderSaga extends Saga {

    private final AggregateStore aggregateStore;

    public OrderSaga(@Nonnull JEventStore store, @Nonnull Offset offset, @Nonnull CommandBus bus,
                     @Nonnull LockManager lockManager, @Nonnull AggregateStore aggregateStore) {
        super(store, offset, bus, lockManager);
        this.aggregateStore = aggregateStore;
    }

    @ReactsOn
    @SuppressWarnings("unused")
    private void reactOn(OrderPlaced event) {
        final Item item = aggregateStore.readBy(event.uuid(), Item.class);
        if (item.isSoldOut()) {
            dispatch(new RemoveFromStock(item.uuid()));
        }
    }
}
