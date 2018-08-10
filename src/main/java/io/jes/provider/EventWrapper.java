package io.jes.provider;

import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import io.jes.Event;

class EventWrapper implements Event {

    private final long id;
    private final Event event;

    EventWrapper(long id, @Nonnull Event event) {
        this.id = id;
        this.event = Objects.requireNonNull(event, "Event can't be null");
    }

    long id() {
        return id;
    }

    @Nullable
    @Override
    public String stream() {
        return event.stream();
    }

    @Override
    public long streamVersion() {
        return event.streamVersion();
    }

    @Nonnull
    Event unwrap() {
        return event;
    }
}
