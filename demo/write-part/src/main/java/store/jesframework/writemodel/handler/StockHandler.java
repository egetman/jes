package store.jesframework.writemodel.handler;

import java.util.UUID;
import javax.annotation.Nonnull;

import org.springframework.stereotype.Component;

import store.jesframework.JEventStore;
import store.jesframework.bus.CommandBus;
import store.jesframework.handler.CommandHandler;
import store.jesframework.handler.Handle;
import store.jesframework.writemodel.command.CreateItem;
import store.jesframework.writemodel.command.RemoveFromStock;
import store.jesframework.writemodel.event.ItemCreated;
import store.jesframework.writemodel.event.ItemRemoved;

@Component
public class StockHandler extends CommandHandler {

    public StockHandler(@Nonnull JEventStore store, @Nonnull CommandBus bus) {
        super(store, bus);
    }

    @Handle
    @SuppressWarnings("unused")
    private void handle(CreateItem command) {
        store.write(new ItemCreated(command.getItemName(), command.getQuantity(), UUID.randomUUID()));
    }

    @Handle
    @SuppressWarnings("unused")
    private void handle(RemoveFromStock command) {
        store.write(new ItemRemoved(command.getItemUuid()));
    }
}
