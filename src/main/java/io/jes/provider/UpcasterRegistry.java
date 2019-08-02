package io.jes.provider;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;

import io.jes.serializer.upcaster.Upcaster;
import lombok.extern.slf4j.Slf4j;

import static java.util.Objects.requireNonNull;

@Slf4j
class UpcasterRegistry<T> {

    /**
     * A little hack to faster resolve type name. All json events (for now) starts with {"@type":"
     */
    private static final int START_TYPE_NAME_POSITION = 10;
    private static final int DEFAULT_NAME_SIZE = 60;

    // no need of concurrent one
    @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
    private final Map<String, Upcaster<T>> upcasters = new HashMap<>();

    void addUpcaster(@Nonnull Upcaster<T> upcaster) {
        requireNonNull(upcaster, "Upcaster must not be null");
        upcasters.put(requireNonNull(upcaster.eventTypeName(), "EventTypeName mmust not be null"), upcaster);
    }

    T tryUpcast(long offset, T raw) {
        if (upcasters.isEmpty()) {
            return raw;
        }
        final String typeName = resolveEventTypeName(raw);
        log.trace("Resolved event type {}", typeName);
        final Upcaster<T> upcaster = upcasters.get(typeName);
        if (upcaster != null) {
            return requireNonNull(upcaster.upcast(offset, raw), "Upcaster must not return null");
        }
        return raw;
    }

    private String resolveEventTypeName(T raw) {
        int size = 0;
        final char[] searched = ((String) raw).toCharArray();
        char[] typeNameArray = new char[DEFAULT_NAME_SIZE];
        for (int i = START_TYPE_NAME_POSITION; i < searched.length; i++) {
            if (searched[i] != '"') {
                typeNameArray[size++] = searched[i];
            } else {
                break;
            }
        }
        return new String(typeNameArray, 0, size);
    }
}
