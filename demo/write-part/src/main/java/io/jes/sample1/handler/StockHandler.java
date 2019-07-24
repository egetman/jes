package io.jes.sample1.handler;

import java.util.UUID;
import javax.annotation.Nonnull;

import org.springframework.stereotype.Component;

import io.jes.JEventStore;
import io.jes.bus.CommandBus;
import io.jes.handler.CommandHandler;
import io.jes.handler.Handle;
import io.jes.sample1.command.CreateItem;
import io.jes.sample1.command.RemoveFromStock;
import io.jes.sample1.event.ItemCreated;
import io.jes.sample1.event.ItemRemoved;

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
