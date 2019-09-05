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
 * Common system event that indicates stream merge.
 */
@Immutable
@EqualsAndHashCode
public class StreamMergedTo implements Event {

    private final UUID uuid;
    @Getter
    private final UUID mergedTo;

    @ConstructorProperties({"uuid", "mergedTo"})
    public StreamMergedTo(@Nonnull UUID uuid, @Nonnull UUID mergedTo) {
        this.uuid = Objects.requireNonNull(uuid, "Source uuid must not be null");
        this.mergedTo = Objects.requireNonNull(mergedTo, "Target uuid must not be null");
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
        sb.append(", mergedTo: ").append(mergedTo);
        sb.append(']');
        return sb.toString();
    }
}
