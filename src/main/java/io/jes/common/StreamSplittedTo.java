package io.jes.common;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import io.jes.Event;
import lombok.EqualsAndHashCode;

/**
 * Common system event that indicates stream split.
 */
@Immutable
@EqualsAndHashCode
public final class StreamSplittedTo implements Event {

    private final UUID uuid;
    private final Set<UUID> splittedTo;

    public StreamSplittedTo(@Nonnull UUID uuid, @Nonnull Set<UUID> splittedTo) {
        this.uuid = Objects.requireNonNull(uuid, "Source uuid must not be null");
        this.splittedTo = new HashSet<>(Objects.requireNonNull(splittedTo, "Target uuids must not be null"));
        if (this.splittedTo.isEmpty()) {
            throw new IllegalArgumentException("Target uuids must not be empty");
        }
    }

    @Nonnull
    @Override
    public UUID uuid() {
        return uuid;
    }

    @Nonnull
    @SuppressWarnings("unused")
    public Set<UUID> getSplittedTo() {
        return new HashSet<>(splittedTo);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder(getClass().getSimpleName() + " [");
        sb.append("uuid: ").append(uuid);
        sb.append(", splittedTo: ").append(splittedTo);
        sb.append(']');
        return sb.toString();
    }

}
