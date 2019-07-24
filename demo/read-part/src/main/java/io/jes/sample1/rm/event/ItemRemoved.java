package io.jes.sample1.rm.event;

import java.beans.ConstructorProperties;
import java.util.UUID;
import javax.annotation.Nonnull;

import io.jes.Event;

public class ItemRemoved implements Event {

    private final UUID itemUuid;

    @ConstructorProperties({"itemUuid"})
    public ItemRemoved(UUID itemUuid) {
        this.itemUuid = itemUuid;
    }

    @Nonnull
    @Override
    public UUID uuid() {
        return itemUuid;
    }
}
