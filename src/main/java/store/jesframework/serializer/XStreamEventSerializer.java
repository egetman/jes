package store.jesframework.serializer;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import store.jesframework.Event;
import store.jesframework.serializer.api.EventSerializer;

public class XStreamEventSerializer extends XStreamSerializer<Event> implements EventSerializer<String> {

    public XStreamEventSerializer(@Nullable TypeRegistry typeRegistry) {
        super(typeRegistry);
    }

    @Nullable
    @Override
    public String fetchTypeName(@Nonnull String raw) {
        // TODO: fetch event type name
        return null;
    }
}
