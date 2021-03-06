package store.jesframework.common;

import java.beans.ConstructorProperties;
import java.util.Objects;
import java.util.UUID;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import store.jesframework.Event;

@EqualsAndHashCode
public class ContextUpdated implements Event {

    private final UUID uuid;
    @Getter
    private final String key;
    @Getter
    private final Object value;
    private final long version;

    @ConstructorProperties({"uuid", "key", "value", "version"})
    public ContextUpdated(@Nonnull UUID uuid, @Nonnull String key, @Nullable Object value, long version) {
        this.uuid = Objects.requireNonNull(uuid, "UUID must not be null");
        this.key = Objects.requireNonNull(key, "Key must not be null");
        this.value = value;
        this.version = version;
    }

    @Nullable
    @Override
    public UUID uuid() {
        return uuid;
    }

    @Override
    public long expectedStreamVersion() {
        return version;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder(getClass().getSimpleName() + " [");
        sb.append("uuid: ").append(uuid);
        sb.append(", key: ").append(key);
        sb.append(", value: ").append(value);
        sb.append(", version: ").append(version);
        sb.append(']');
        return sb.toString();
    }
}
