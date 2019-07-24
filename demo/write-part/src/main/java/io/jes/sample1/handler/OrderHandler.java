package io.jes.sample1.handler;

import java.time.LocalDateTime;
import javax.annotation.Nonnull;

import org.springframework.stereotype.Component;

import io.jes.AggregateStore;
import io.jes.JEventStore;
import io.jes.bus.CommandBus;
import io.jes.handler.CommandHandler;
import io.jes.handler.Handle;
import io.jes.sample1.command.PlaceOrder;
import io.jes.sample1.domain.Item;
import io.jes.sample1.event.OrderPlaced;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class OrderHandler extends CommandHandler {

    private final AggregateStore aggregateStore;

    public OrderHandler(@Nonnull JEventStore store, @Nonnull CommandBus bus, @Nonnull AggregateStore aggregateStore) {
        super(store, bus);
        this.aggregateStore = aggregateStore;
    }

    @Handle
    @SuppressWarnings("unused")
    private void handle(PlaceOrder command) {
        log.info("Prepare to place order {}", command);
        final Item item = aggregateStore.readBy(command.getItemUuid(), Item.class);
        if (item.getQuantity() < command.getQuantity()) {
            // u can throw any validation error here, or emit domain event
            throw new IllegalStateException("Out of stock");
        } else {
            store.write(new OrderPlaced(command.getItemUuid(), command.getQuantity(), LocalDateTime.now()));
        }
    }

}
