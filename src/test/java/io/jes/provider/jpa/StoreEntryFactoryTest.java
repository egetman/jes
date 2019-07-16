package io.jes.provider.jpa;

import java.util.UUID;

import org.junit.jupiter.api.Test;

import io.jes.ex.SerializationException;

import static io.jes.provider.jpa.StoreEntry.StoreBinaryEntry;
import static io.jes.provider.jpa.StoreEntry.StoreStringEntry;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class StoreEntryFactoryTest {

    @Test
    @SuppressWarnings("ConstantConditions")
    void factoryShouldThrowNullPointerExceptionOnNullArguments() {
        assertThrows(NullPointerException.class, () -> StoreEntryFactory.entryTypeOf(null));
        assertThrows(NullPointerException.class, () -> StoreEntryFactory.newEntry(UUID.randomUUID(), null));
    }

    @Test
    void factoryShouldNotThrowNullPointerExceptionOnNullUuid() {
        assertDoesNotThrow(() -> StoreEntryFactory.newEntry(null, "Foo"));
    }

    @Test
    void factoryShouldThrowSerializationExceptionOnUnknownType() {
        assertThrows(SerializationException.class, () -> StoreEntryFactory.entryTypeOf(UUID.class));
        assertThrows(SerializationException.class, () -> StoreEntryFactory.newEntry(UUID.randomUUID(), Class.class));
    }

    @Test
    void factoryShouldReturnCorrectEntryTypeBasedOnInputType() {
        assertEquals(StoreBinaryEntry.class, StoreEntryFactory.entryTypeOf(byte[].class));
        assertEquals(StoreStringEntry.class, StoreEntryFactory.entryTypeOf(String.class));
    }

    @Test
    void factoryShouldReturnCorrectEntryBasedOnInputParameters() {
        final UUID uuid = UUID.randomUUID();

        final String stringPayload = "";
        final byte[] bytePayload = new byte[0];

        assertEquals(new StoreBinaryEntry(uuid, bytePayload), StoreEntryFactory.newEntry(uuid, bytePayload));
        assertEquals(new StoreStringEntry(uuid, stringPayload), StoreEntryFactory.newEntry(uuid, stringPayload));
    }

}