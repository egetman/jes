package store.jesframework;

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

    @SuppressWarnings("squid:S2065")
    private final transient Map<Class<? extends Event>, Consumer<? extends Event>> appliers = new HashMap<>();

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
        requireNonNull(stream, "Event stream must not be null");
        for (Event event : stream) {
            final Consumer<Event> applier = applierFor(event.getClass());
            if (applier != null) {
                applier.accept(event);
            }
        }
        streamVersion += stream.size();
    }

    @Nullable
    private Consumer<Event> applierFor(@Nonnull Class<? extends Event> type) {
        requireNonNull(type, "Event type must not be null");
        @SuppressWarnings("unchecked")
        final Consumer<Event> consumer = (Consumer<Event>) appliers.get(type);
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
