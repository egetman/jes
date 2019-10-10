package store.jesframework.writemodel.handler;

import java.time.LocalDateTime;
import javax.annotation.Nonnull;

import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;
import store.jesframework.AggregateStore;
import store.jesframework.bus.CommandBus;
import store.jesframework.handler.CommandHandler;
import store.jesframework.handler.Handle;
import store.jesframework.writemodel.command.PlaceOrder;
import store.jesframework.writemodel.domain.Item;
import store.jesframework.writemodel.event.OrderPlaced;

@Slf4j
@Component
public class OrderHandler extends CommandHandler {

    public OrderHandler(@Nonnull AggregateStore aggregateStore, @Nonnull CommandBus bus) {
        super(aggregateStore, bus);
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
