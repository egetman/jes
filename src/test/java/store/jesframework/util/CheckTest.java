package store.jesframework.util;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;

import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.NaN;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@Execution(CONCURRENT)
class CheckTest {

    private static class FooException extends RuntimeException {}

    @Test
    @SuppressWarnings("ConstantConditions")
    void shouldCorrectlyVerifyCollectionWithNonEmpty() {
        final Map<?, ?> nullMap = null;
        final Object[] nullArray = null;
        final Collection<?> nullCollection = null;

        assertThrows(FooException.class, () -> Check.nonEmpty(nullMap, FooException::new));
        assertThrows(FooException.class, () -> Check.nonEmpty(nullArray, FooException::new));
        assertThrows(FooException.class, () -> Check.nonEmpty(nullCollection, FooException::new));

        assertThrows(FooException.class, () -> Check.nonEmpty(new Object[0], FooException::new));

        final List<Object> emptyList = Collections.emptyList();
        final Map<Object, Object> emptyMap = Collections.emptyMap();
        assertThrows(FooException.class, () -> Check.nonEmpty(emptyMap, FooException::new));
        assertThrows(FooException.class, () -> Check.nonEmpty(emptyList, FooException::new));

        assertThrows(NullPointerException.class, () -> Check.nonEmpty(nullMap, () -> null));
        assertThrows(NullPointerException.class, () -> Check.nonEmpty(nullArray, () -> null));
        assertThrows(NullPointerException.class, () -> Check.nonEmpty(nullCollection, () -> null));

        final Object[] array = {"FOO"};
        final Set<String> collection = Collections.singleton("FOO");
        final Map<String, String> map = Collections.singletonMap("FOO", "BAR");

        assertThrows(NullPointerException.class, () -> Check.nonEmpty(map, null));
        assertThrows(NullPointerException.class, () -> Check.nonEmpty(array, null));
        assertThrows(NullPointerException.class, () -> Check.nonEmpty(collection, null));

        assertDoesNotThrow(() -> Check.nonEmpty(map, FooException::new));
        assertDoesNotThrow(() -> Check.nonEmpty(array, FooException::new));
        assertDoesNotThrow(() -> Check.nonEmpty(collection, FooException::new));
    }

    @Test
    @SuppressWarnings("ConstantConditions")
    void shouldValidateNonEqualObjects() {
        assertThrows(NullPointerException.class, () -> Check.nonEqual("", "b", null));
        assertThrows(NullPointerException.class, () -> Check.nonEqual("", "", null));

        assertThrows(FooException.class, () -> Check.nonEqual(1, 1, FooException::new));

        assertDoesNotThrow(() -> Check.nonEqual(NaN, NEGATIVE_INFINITY, FooException::new));
    }

}