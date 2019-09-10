package store.jesframework.readmodel.consumer;

import javax.annotation.Nonnull;

import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

import store.jesframework.JEventStore;
import store.jesframework.lock.Lock;
import store.jesframework.offset.Offset;
import store.jesframework.reactors.Projector;
import store.jesframework.reactors.ReactsOn;
import store.jesframework.readmodel.event.ItemCreated;
import store.jesframework.readmodel.event.ItemRemoved;
import store.jesframework.readmodel.event.OrderPlaced;
import store.jesframework.readmodel.model.Item;
import store.jesframework.readmodel.repository.ItemRepository;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class StockProjector extends Projector {

    private final ItemRepository itemRepository;

    public StockProjector(@Nonnull JEventStore store, @Nonnull Offset offset, @Nonnull Lock lock,
                          @Nonnull ItemRepository itemRepository) {
        super(store, offset, lock);
        this.itemRepository = itemRepository;
    }

    @ReactsOn
    @SuppressWarnings("unused")
    void reactOn(ItemCreated event) {
        Item item = new Item();
        item.setUuid(event.uuid());
        item.setName(event.getItemName());
        item.setQuantity(event.getQuantity());
        itemRepository.save(item);
    }

    @ReactsOn
    @SuppressWarnings("unused")
    void reactOn(OrderPlaced event) {
        final Item item = itemRepository.findByUuid(event.uuid());
        item.setQuantity(item.getQuantity() - event.getQuantity());
        item.setLastOrdered(event.getWhen());
        itemRepository.save(item);
    }

    @ReactsOn
    @SuppressWarnings("unused")
    void reactOn(ItemRemoved event) {
        final Item item = itemRepository.findByUuid(event.uuid());
        if (item != null) {
            itemRepository.delete(item);
        }
    }

    @Override
    protected void cleanUp() {
        log.info("Recreating current projection");
        itemRepository.deleteAll();
        log.info("Projection recreated");
    }

    @Override
    @EventListener(ContextClosedEvent.class)
    public void close() {
        super.close();
    }
}
