package store.jesframework.common;

import java.beans.ConstructorProperties;
import java.time.LocalDateTime;
import java.util.Objects;
import java.util.UUID;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import store.jesframework.Event;

/**
 * Common system event that indicates handling failure.
 */
@Getter
@ToString
@EqualsAndHashCode
class Failure implements Event {

    private final UUID uuid;

    private final long offset;
    private final Event source;
    private final String cause;
    private final String byWhom;
    /**
     * A date-time of the incident. Default is #now().
     */
    private final LocalDateTime when = LocalDateTime.now();

    /**
     * Constructor for {@literal Failure} event.
     *
     * @param source is a failed to process event.
     * @param byWhom client identifier (key).
     * @param offset is an offset number of {@literal source} event.
     * @param cause  is a description of incident.
     */
    @ConstructorProperties({"source", "byWhom", "offset", "cause"})
    Failure(@Nonnull Event source, @Nonnull String byWhom, long offset, @Nullable String cause) {
        this.cause = cause;
        this.offset = offset;
        this.source = Objects.requireNonNull(source, "Failed source must not be null");
        this.byWhom = Objects.requireNonNull(byWhom, "Client identifier must not be null");
        this.uuid = source.uuid();
    }

    @Nullable
    @Override
    public UUID uuid() {
        return uuid;
    }

}
