package io.jes.sample1.saga;

import java.util.Objects;
import javax.annotation.Nonnull;

import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import io.jes.AggregateStore;
import io.jes.JEventStore;
import io.jes.bus.CommandBus;
import io.jes.lock.Lock;
import io.jes.offset.Offset;
import io.jes.reactors.ReactsOn;
import io.jes.reactors.Saga;
import io.jes.sample1.command.RemoveFromStock;
import io.jes.sample1.domain.Item;
import io.jes.sample1.event.OrderPlaced;

@Component
public class OrderSaga extends Saga {

    private final CommandBus bus;
    private final AggregateStore aggregateStore;

    public OrderSaga(@Nonnull JEventStore store, @Nonnull Offset offset, @Nonnull Lock lock, @Nonnull CommandBus bus,
                     @Nonnull AggregateStore aggregateStore) {
        super(store, offset, lock);
        this.bus = Objects.requireNonNull(bus, "CommandBus must not be null");
        this.aggregateStore = Objects.requireNonNull(aggregateStore, "AggregateStore must not be null");
    }

    @ReactsOn
    @SuppressWarnings("unused")
    private void reactOn(OrderPlaced event) {
        final Item item = aggregateStore.readBy(event.uuid(), Item.class);
        if (item.isSoldOut()) {
            bus.dispatch(new RemoveFromStock(item.uuid()));
        }
    }

    @Override
    @EventListener(ContextClosedEvent.class)
    public void close() {
        super.close();
    }
}
