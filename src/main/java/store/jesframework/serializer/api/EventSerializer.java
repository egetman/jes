package store.jesframework.serializer.api;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import store.jesframework.Event;

public interface EventSerializer<T> extends Serializer<Event, T> {

    /**
     * This method tries to retrieve event name from the specified 'raw' event data.
     *
     * @param raw is a event serialized form.
     * @return the name of given event or null, if can't fetch it.
     */
    @Nullable
    String fetchTypeName(@Nonnull T raw);

}
