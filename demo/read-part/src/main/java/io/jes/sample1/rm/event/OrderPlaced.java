package io.jes.sample1.rm.event;

import java.beans.ConstructorProperties;
import java.time.LocalDateTime;
import java.util.UUID;
import javax.annotation.Nonnull;

import io.jes.Event;
import lombok.Getter;

@Getter
public class OrderPlaced implements Event {

    private final UUID itemUuid;
    private final long quantity;
    private final LocalDateTime when;

    @ConstructorProperties({"itemUuid", "quantity", "when"})
    public OrderPlaced(UUID itemUuid, long quantity, LocalDateTime when) {
        this.itemUuid = itemUuid;
        this.quantity = quantity;
        this.when = when;
    }

    @Nonnull
    @Override
    public UUID uuid() {
        return itemUuid;
    }
}
