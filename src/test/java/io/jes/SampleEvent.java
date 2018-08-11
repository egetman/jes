package io.jes;

import java.util.UUID;

import lombok.EqualsAndHashCode;

@EqualsAndHashCode
class SampleEvent implements Event {

    private final String name;
    private final String stream;
    private final long expectedStreamVersion;

    SampleEvent(String name) {
        this(name, UUID.randomUUID().toString());
    }

    SampleEvent(String name, String stream) {
        this(name, stream, -1);
    }

    SampleEvent(String name, String stream, long expectedStreamVersion) {
        this.name = name;
        this.stream = stream;
        this.expectedStreamVersion = expectedStreamVersion;
    }

    @Override
    public String stream() {
        return stream;
    }

    @Override
    public long expectedStreamVersion() {
        return expectedStreamVersion;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder(getClass().getSimpleName() + " [");
        sb.append("name: ").append(name);
        sb.append(", stream: ").append(stream);
        sb.append(']');
        return sb.toString();
    }
}