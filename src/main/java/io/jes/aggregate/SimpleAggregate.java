package io.jes.aggregate;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Consumer;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import io.jes.Event;
import lombok.extern.slf4j.Slf4j;

import static java.util.Objects.requireNonNull;

@Slf4j
public class SimpleAggregate implements Aggregate {

    private static final Map<Class<? extends Event>, Consumer<? extends Event>> APPLIERS = new HashMap<>();

    protected UUID uuid;

    @Nonnull
    @Override
    public UUID uuid() {
        return Objects.requireNonNull(uuid, "Aggregate#uuid must not be null");
    }

    @Nullable
    @Override
    public <T extends Event> Consumer<T> applierFor(@Nonnull Class<T> type) {
        @SuppressWarnings("unchecked")
        final Consumer<T> consumer = (Consumer<T>) APPLIERS.get(requireNonNull(type, "Event type must not be null"));
        if (consumer == null) {
            log.trace("Aggregate {} doesn't have a registered {} applier", getClass().getName(), type.getName());
        }
        return consumer;
    }

    protected <T extends Event> void registerApplier(@Nonnull Class<T> type, @Nonnull Consumer<T> logic) {
        APPLIERS.put(
                requireNonNull(type, "Event type must not be null"),
                requireNonNull(logic, "Registered domain logic must not be null")
        );
    }
}
