package io.jes.util;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public final class Check {

    private Check() {
    }

    /**
     * Verify that given collection not null or empty.
     *
     * @param elements collection to check against emptiness.
     * @param supplier the exception producer.
     */
    public static void nonEmpty(@Nullable Collection<?> elements,
                                @Nonnull Supplier<? extends RuntimeException> supplier) {
        if (elements == null || elements.isEmpty()) {
            throw supplier.get();
        }
    }

    /**
     * Verify that given map not null or empty.
     *
     * @param elements map to check against emptiness.
     * @param supplier the exception producer.
     */
    public static void nonEmpty(@Nullable Map<?, ?> elements, @Nonnull Supplier<? extends RuntimeException> supplier) {
        if (elements == null || elements.isEmpty()) {
            throw supplier.get();
        }
    }

    /**
     * Verify that arguments are not equal to each other.
     *
     * @param left     an object.
     * @param right    an object to be compared with {@code left} for equality.
     * @param supplier the exception producer.
     */
    public static void nonEqual(@Nullable Object left, @Nullable Object right,
                                @Nonnull Supplier<? extends RuntimeException> supplier) {
        if (Objects.equals(left, right)) {
            throw supplier.get();
        }
    }

}
