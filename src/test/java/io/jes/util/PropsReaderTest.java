package io.jes.util;

import org.junit.jupiter.api.Test;

import io.jes.ex.PropertyNotFoundException;

import static org.junit.jupiter.api.Assertions.*;

class PropsReaderTest {

    @Test
    void shouldThrowNullPointerExceptionOnNullPropertyName() {
        //noinspection ConstantConditions
        assertThrows(NullPointerException.class, () -> PropsReader.getPropety(null));
    }

    @Test
    void shouldFindSystemPropertyIfExists() {
        System.setProperty("jes.foo", "jes.baz");
        assertEquals("jes.baz", PropsReader.getPropety("jes.foo"));
        // once again
        assertEquals("jes.baz", PropsReader.getPropety("jes.foo"));
    }

    @Test
    void shouldFindPropertyFromPropertyFileIfExists() {
        assertEquals("es", PropsReader.getPropety("jes.jdbc.schema-name"));
        // once again
        assertEquals("es", PropsReader.getPropety("jes.jdbc.schema-name"));
    }

    @Test
    void shouldThrowPropertyNotFoundExceptionOnMissingProperty() {
        final PropertyNotFoundException exception = assertThrows(PropertyNotFoundException.class,
                () -> PropsReader.getPropety("bazz"));

        assertEquals("Can't find specified property: bazz in system properties and jes.properties file",
                exception.getMessage());
    }

}