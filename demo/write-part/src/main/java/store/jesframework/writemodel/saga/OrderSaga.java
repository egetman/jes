package store.jesframework.writemodel.saga;

import java.util.Objects;
import javax.annotation.Nonnull;

import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

import store.jesframework.AggregateStore;
import store.jesframework.bus.CommandBus;
import store.jesframework.lock.Lock;
import store.jesframework.offset.Offset;
import store.jesframework.reactors.ReactsOn;
import store.jesframework.reactors.Saga;
import store.jesframework.writemodel.command.RemoveFromStock;
import store.jesframework.writemodel.domain.Item;
import store.jesframework.writemodel.event.OrderPlaced;

@Service
public class OrderSaga extends Saga {

    private final CommandBus bus;
    private final AggregateStore aggregateStore;

    public OrderSaga(@Nonnull AggregateStore aggregateStore, @Nonnull Offset offset, @Nonnull Lock lock,
                     @Nonnull CommandBus bus) {
        super(aggregateStore, offset, lock);
        this.bus = Objects.requireNonNull(bus, "CommandBus must not be null");
        this.aggregateStore = aggregateStore;
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
