package io.jes.sample1.domain;

import io.jes.Aggregate;
import io.jes.sample1.event.ItemCreated;
import io.jes.sample1.event.OrderPlaced;
import lombok.Getter;

@Getter
public class Item extends Aggregate {

    private String name;
    private long quantity;

    public Item() {
        registerApplier(ItemCreated.class, this::apply);
        registerApplier(OrderPlaced.class, this::apply);
    }

    private void apply(ItemCreated event) {
        uuid = event.uuid();
        name = event.getItemName();
        quantity = event.getQuantity();
    }

    private void apply(OrderPlaced event) {
        quantity -= event.getQuantity();
    }

    public boolean isSoldOut() {
        return quantity == 0;
    }
}
