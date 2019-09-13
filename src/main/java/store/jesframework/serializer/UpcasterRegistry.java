package store.jesframework.serializer;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import store.jesframework.serializer.api.Upcaster;
import lombok.extern.slf4j.Slf4j;

import static java.util.Objects.requireNonNull;

@Slf4j
class UpcasterRegistry<T> {

    // no need of concurrent one
    @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
    private final Map<String, Upcaster<T>> upcasters = new HashMap<>();

    void addUpcaster(@Nonnull Upcaster<T> upcaster) {
        requireNonNull(upcaster, "Upcaster must not be null");
        upcasters.put(requireNonNull(upcaster.eventTypeName(), "EventTypeName mmust not be null"), upcaster);
    }

    @Nonnull
    T tryUpcast(@Nonnull T raw, @Nullable String typeName) {
        if (upcasters.isEmpty() || typeName == null) {
            return raw;
        }
        log.trace("Resolved event type {}", typeName);
        final Upcaster<T> upcaster = upcasters.get(typeName);
        if (upcaster != null) {
            try {
                return requireNonNull(upcaster.upcast(raw), "Upcaster must not return null");
            } catch (Exception e) {
                log.error("Failed to upcast raw type: {}", raw, e);
            }
        }
        return raw;
    }

}
