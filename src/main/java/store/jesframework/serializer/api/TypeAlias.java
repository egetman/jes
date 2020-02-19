package store.jesframework.serializer.api;

import java.util.Objects;
import javax.annotation.concurrent.Immutable;

import lombok.AccessLevel;
import lombok.Data;
import lombok.RequiredArgsConstructor;

/**
 * Aliasing strategy for specific type.
 *
 * @see AliasingStrategy
 * @see Serializer#fetchTypeName(Object)
 */
@Data
@Immutable
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class TypeAlias implements SerializationOption {

    private final String alias;
    private final Class<?> type;

    /**
     * Create an aliasing rule for specified {@code type}.
     *
     * @param type  is a class to be aliased.
     * @param alias is an alias name.
     * @return constructed {@link TypeAlias} instance.
     * @see SerializationOption
     * @see store.jesframework.serializer.impl.Context
     */
    public static TypeAlias of(Class<?> type, String alias) {
        Objects.requireNonNull(type, "Type must not be null");
        Objects.requireNonNull(alias, "Alias must not be null");
        return new TypeAlias(alias, type);
    }

    public static TypeAlias ofShortName(Class<?> type) {
        return of(type, type.getSimpleName());
    }

}
