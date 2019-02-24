package io.jes.provider.jpa;

import java.util.Objects;
import java.util.UUID;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import io.jes.ex.SerializationException;

public class StoreEntryFactory {

    private StoreEntryFactory() {}

    /**
     * Resolves and return entry type based on provided {@literal payloadClass} type.
     */
    public static Class<? extends StoreEntry> entryTypeOf(@Nonnull Class<?> payloadClass) {
        if (String.class == payloadClass) {
            return StoreEntry.StoreStringEntry.class;
        } else if (byte[].class == payloadClass) {
            return StoreEntry.StoreBinaryEntry.class;
        }
        throw new SerializationException("Payload of type " + payloadClass + " cannot be processed");
    }

    /**
     * Factory mathod for creatig new {@literal store entry} based on paeload type.
     */
    public static StoreEntry newEntry(@Nullable UUID uuid, @Nonnull Object payload) {
        Objects.requireNonNull(payload, "Event payload must not be null");
        if (payload instanceof String) {
            return new StoreEntry.StoreStringEntry(uuid, (String) payload);
        } else if (payload instanceof byte[]) {
            return new StoreEntry.StoreBinaryEntry(uuid, (byte[]) payload);
        }
        throw new SerializationException("Payload of type " + payload.getClass() + " cannot be processed");
    }

}
