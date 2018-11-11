package io.jes.aggregate;

import java.util.UUID;
import javax.annotation.Nonnull;

import io.jes.Event;

public interface AggregateStore {

    @Nonnull
    <T extends Aggregate> T readBy(@Nonnull UUID uuid, Class<T> type);

    void write(@Nonnull Event event);
}
