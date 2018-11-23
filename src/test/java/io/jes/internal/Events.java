package io.jes.internal;

import java.util.Objects;
import java.util.UUID;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import io.jes.Event;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;

public final class Events {

    private Events() {}

    @RequiredArgsConstructor
    @NoArgsConstructor(force = true)
    public static class ProcessingStarted implements Event {
        private final UUID uuid;

        @Nullable
        @Override
        public UUID uuid() {
            return uuid;
        }
    }

    @RequiredArgsConstructor
    @NoArgsConstructor(force = true)
    public static class ProcessingTerminated implements Event {

        private final UUID uuid;

        @Nullable
        @Override
        public UUID uuid() {
            return uuid;
        }
    }

    @EqualsAndHashCode
    public static class FancyEvent implements Event {

        private final UUID uuid;
        @Getter
        private final String name;

        public FancyEvent(@Nonnull String name, @Nonnull UUID uuid) {
            this.name = Objects.requireNonNull(name);
            this.uuid = Objects.requireNonNull(uuid);
        }

        @Nullable
        @Override
        public UUID uuid() {
            return uuid;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder(getClass().getSimpleName() + " [");
            sb.append("name: ").append(name);
            sb.append(", uuid: ").append(uuid);
            sb.append(']');
            return sb.toString();
        }
    }

    @EqualsAndHashCode
    public static class SampleEvent implements Event {

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
}