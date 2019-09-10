package store.jesframework.readmodel.event;

import java.beans.ConstructorProperties;
import java.util.UUID;
import javax.annotation.Nonnull;

import store.jesframework.Event;

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
