package io.jes.common;

import java.beans.ConstructorProperties;
import java.util.Objects;
import java.util.UUID;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import io.jes.Event;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * Common system event that indicates stream replacement.
 */
@Immutable
@EqualsAndHashCode
public final class StreamMovedTo implements Event {

    private final UUID uuid;
    @Getter
    private final UUID movedTo;

    @ConstructorProperties({"uuid", "movedTo"})
    public StreamMovedTo(@Nonnull UUID uuid, @Nonnull UUID movedTo) {
        this.uuid = Objects.requireNonNull(uuid, "Source uuid must not be null");
        this.movedTo = Objects.requireNonNull(movedTo, "Target uuid must not be null");
    }

    @Nonnull
    @Override
    public UUID uuid() {
        return uuid;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder(getClass().getSimpleName() + " [");
        sb.append("uuid: ").append(uuid);
        sb.append(", movedTo: ").append(movedTo);
        sb.append(']');
        return sb.toString();
    }
}
