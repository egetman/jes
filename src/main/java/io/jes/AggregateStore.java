package io.jes;

import java.util.UUID;
import javax.annotation.Nonnull;

public interface AggregateStore {

    @Nonnull
    <T extends Aggregate> T readBy(@Nonnull UUID uuid, @Nonnull Class<T> type);

}
