package io.jes.common;

import java.util.Objects;
import javax.annotation.Nonnull;

import io.jes.Event;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode
public class UnknownTypeResolved implements Event {

    private final String type;
    private final Object raw;

    public UnknownTypeResolved(@Nonnull String type, @Nonnull Object raw) {
        this.type = Objects.requireNonNull(type);
        this.raw = Objects.requireNonNull(raw);
    }

    /**
     * Return the type of deserialized unregistered event.
     *
     * @return type of unregistered event.
     */
    public String type() {
        return type;
    }

    /**
     * Return event in raw form from store.
     *
     * @param <T> type of raw event.
     * @return raw event from store.
     */
    public <T> T raw() {
        //noinspection unchecked
        return (T) raw;
    }
}
