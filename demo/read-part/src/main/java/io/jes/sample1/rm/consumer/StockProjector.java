package io.jes.sample1.rm.consumer;

import javax.annotation.Nonnull;

import org.springframework.stereotype.Service;

import io.jes.JEventStore;
import io.jes.lock.LockManager;
import io.jes.offset.Offset;
import io.jes.reactors.Projector;
import io.jes.reactors.ReactsOn;
import io.jes.sample1.rm.event.ItemCreated;
import io.jes.sample1.rm.event.ItemRemoved;
import io.jes.sample1.rm.event.OrderPlaced;
import io.jes.sample1.rm.model.Item;
import io.jes.sample1.rm.repository.ItemRepository;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class StockProjector extends Projector {

    private final ItemRepository itemRepository;

    public StockProjector(@Nonnull JEventStore store, @Nonnull Offset offset, @Nonnull LockManager lockManager,
                          @Nonnull ItemRepository itemRepository) {
        super(store, offset, lockManager);
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
    protected void onRecreate() {
        log.info("Recreating current projection");
        itemRepository.deleteAll();
        log.info("Projection recreated");
    }

    @Override
    public void close() {
        super.close();
        recreate();
    }
}
