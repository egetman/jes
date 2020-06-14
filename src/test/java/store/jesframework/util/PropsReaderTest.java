package store.jesframework.util;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;

import store.jesframework.ex.PropertyNotFoundException;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@Execution(CONCURRENT)
class PropsReaderTest {

    @Test
    void shouldThrowNullPointerExceptionOnNullPropertyName() {
        //noinspection ConstantConditions
        assertThrows(NullPointerException.class, () -> PropsReader.getProperty(null));
    }

    @Test
    void shouldFindSystemPropertyIfExists() {
        System.setProperty("jes.foo", "jes.baz");
        assertEquals("jes.baz", PropsReader.getProperty("jes.foo"));
        // once again
        assertEquals("jes.baz", PropsReader.getProperty("jes.foo"));
    }

    @Test
    void shouldFindPropertyFromPropertyFileIfExists() {
        assertEquals("es", PropsReader.getProperty("jes.jdbc.schema-name"));
        // once again
        assertEquals("es", PropsReader.getProperty("jes.jdbc.schema-name"));
    }

    @Test
    void shouldThrowPropertyNotFoundExceptionOnMissingProperty() {
        final PropertyNotFoundException exception = assertThrows(PropertyNotFoundException.class,
                () -> PropsReader.getProperty("bazz"));

        assertEquals("Can't find specified property: bazz in system properties and jes.properties file",
                exception.getMessage());
    }

}