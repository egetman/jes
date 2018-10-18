package io.jes.common;

import java.util.UUID;

import javax.annotation.Nonnull;

import io.jes.Event;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@EqualsAndHashCode
public class SampleEvent implements Event {

    @Getter
    private final String name;
    private final UUID uuid;
    private final long expectedStreamVersion;

    public SampleEvent(String name) {
        this(name, UUID.randomUUID());
    }

    public SampleEvent(String name, UUID uuid) {
        this(name, uuid, -1);
    }

    public SampleEvent(String name, UUID uuid, long expectedStreamVersion) {
        this.name = name;
        this.uuid = uuid;
        this.expectedStreamVersion = expectedStreamVersion;
    }

    @Nonnull
    @Override
    public UUID uuid() {
        return uuid;
    }

    @Override
    public long expectedStreamVersion() {
        return expectedStreamVersion;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder(getClass().getSimpleName() + " [");
        sb.append("name: ").append(name);
        sb.append(", uuid: ").append(uuid);
        if (expectedStreamVersion != -1) {
            sb.append(", stream version: ").append(expectedStreamVersion);
        }
        sb.append(']');
        return sb.toString();
    }
}