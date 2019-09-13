package store.jesframework.serializer;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import store.jesframework.Event;
import store.jesframework.serializer.api.EventSerializer;

class KryoEventSerializer extends KryoSerializer<Event> implements EventSerializer<byte[]> {

    @Nullable
    @Override
    public String fetchTypeName(@Nonnull byte[] raw) {
        return null;
    }
}
