package store.jesframework.provider.jpa;

import java.util.Objects;
import java.util.UUID;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import store.jesframework.ex.SerializationException;

public class StoreEntryFactory {

    private StoreEntryFactory() {
    }

    /**
     * Resolves and return entry type based on provided {@literal payloadClass} type.
     *
     * @param payloadClass is type of 'raw' event: string or binary.
     * @return {@link StoreEntry} child class for working with given {@literal payloadClass}.
     */
    public static Class<? extends StoreEntry> entryTypeOf(@Nonnull Class<?> payloadClass) {
        Objects.requireNonNull(payloadClass, "Payload class must be specified");
        if (String.class == payloadClass) {
            return StoreEntry.StoreStringEntry.class;
        } else if (byte[].class == payloadClass) {
            return StoreEntry.StoreBinaryEntry.class;
        }
        throw new SerializationException("Payload of type " + payloadClass + " cannot be processed");
    }

    /**
     * Factory mathod for creatig new {@literal store entry} based on paeload type.
     *
     * @param uuid    is uuid of event stream, if present.
     * @param payload is 'raw' event.
     * @return constructed and initialized {@link StoreEntry} with given uuid and payload.
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
