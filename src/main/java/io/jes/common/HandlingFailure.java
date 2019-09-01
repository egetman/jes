package io.jes.common;

import java.time.LocalDateTime;
import java.util.Objects;
import java.util.UUID;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import io.jes.Event;
import lombok.Getter;

/**
 * Common system event that indicates saga handling failure.
 */
public class HandlingFailure implements Event {

    private final UUID uuid;
    @Getter
    private final long offset;
    @Getter
    private final Event source;
    @Getter
    private final String byWhom;
    @Getter
    private final LocalDateTime when;

    /**
     * Constructor for {@literal HandlingFailure} event.
     *
     * @param source is a failed to process event.
     * @param when   is a date-time of the incident.
     * @param byWhom client (saga) identifier (key).
     * @param offset is an offset number of {@literal source} event.
     */
    public HandlingFailure(@Nonnull Event source, @Nonnull LocalDateTime when, @Nonnull String byWhom, long offset) {
        this.when = Objects.requireNonNull(when, "Failure time must not be null");
        this.source = Objects.requireNonNull(source, "Failed source must not be null");
        this.byWhom = Objects.requireNonNull(byWhom, "Client (saga) identifier must not be null");
        this.offset = offset;
        this.uuid = source.uuid();
    }

    @Nullable
    @Override
    public UUID uuid() {
        return uuid;
    }
}
