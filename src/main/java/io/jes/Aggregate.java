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
public class Aggregate {

    private final Map<Class<? extends Event>, Consumer<? extends Event>> appliers = new HashMap<>();

    protected UUID uuid;
    private long streamVersion;

    @Nonnull
    public UUID uuid() {
        return Objects.requireNonNull(uuid, "Aggregate#uuid must not be null");
    }

    public long streamVersion() {
        return streamVersion;
    }

    void handleEventStream(@Nonnull Collection<Event> stream) {
        Objects.requireNonNull(stream, "Event stream must not be null");
        for (Event event : stream) {
            final Class<? extends Event> type = event.getClass();
            @SuppressWarnings({"unchecked", "RedundantCast"})
            final Consumer<Event> applier = (Consumer<Event>) applierFor(type);
            if (applier != null) {
                applier.accept(event);
            }
        }
        streamVersion += stream.size();
    }

    @Nullable
    private <T extends Event> Consumer<T> applierFor(@Nonnull Class<T> type) {
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
