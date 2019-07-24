package io.jes.sample1.rm.event;

import java.beans.ConstructorProperties;
import java.util.UUID;
import javax.annotation.Nonnull;

import io.jes.Event;
import lombok.Getter;

public class ItemCreated implements Event {

    @Getter
    private final String itemName;
    @Getter
    private final long quantity;
    private final UUID itemUuid;

    @ConstructorProperties({"itemName", "quantity", "itemUuid"})
    public ItemCreated(String itemName, long quantity, UUID itemUuid) {
        this.itemName = itemName;
        this.quantity = quantity;
        this.itemUuid = itemUuid;
    }

    @Nonnull
    @Override
    public UUID uuid() {
        return itemUuid;
    }
}
