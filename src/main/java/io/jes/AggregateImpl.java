package io.jes;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Consumer;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import lombok.extern.slf4j.Slf4j;

import static java.util.Objects.requireNonNull;

@Slf4j
public class AggregateImpl implements Aggregate {

    private final Map<Class<? extends Event>, Consumer<? extends Event>> appliers = new HashMap<>();

    protected UUID uuid;
    private long streamVersion;

    @Nonnull
    @Override
    public UUID uuid() {
        return Objects.requireNonNull(uuid, "Aggregate#uuid must not be null");
    }

    @Override
    public long streamVersion() {
        return streamVersion;
    }

    @Override
    public void handleEventStream(@Nonnull Collection<Event> stream) {
        Aggregate.super.handleEventStream(stream);
        streamVersion += stream.size();
    }

    @Nullable
    @Override
    public <T extends Event> Consumer<T> applierFor(@Nonnull Class<T> type) {
        @SuppressWarnings("unchecked")
        final Consumer<T> consumer = (Consumer<T>) appliers.get(requireNonNull(type, "Event type must not be null"));
        if (consumer == null) {
            log.trace("Aggregate {} doesn't have a registered {} applier", getClass().getName(), type.getName());
        }
        return consumer;
    }

    protected <T extends Event> void registerApplier(@Nonnull Class<T> type, @Nonnull Consumer<T> logic) {
        appliers.put(
                requireNonNull(type, "Event type must not be null"),
                requireNonNull(logic, "Registered domain logic must not be null")
        );
    }
}
