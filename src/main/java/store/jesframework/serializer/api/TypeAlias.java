package store.jesframework.serializer.api;

import java.util.Objects;
import javax.annotation.concurrent.Immutable;

import lombok.AccessLevel;
import lombok.Data;
import lombok.RequiredArgsConstructor;

/**
 * Aliasing strategy for specific type.
 * <p>
 * See {@link AliasingStrategy}. See {@link Serializer#fetchTypeName(Object)}.
 */
@Data
@Immutable
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class TypeAlias implements SerializationOption {

    private final String alias;
    private final Class<?> type;

    public static TypeAlias of(Class<?> type, String alias) {
        Objects.requireNonNull(type, "Type must not be null");
        Objects.requireNonNull(alias, "Alias must not be null");
        return new TypeAlias(alias, type);
    }

    public static TypeAlias ofShortName(Class<?> type) {
        return of(type, type.getSimpleName());
    }

}
