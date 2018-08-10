package io.jes;

import java.util.UUID;

class SampleEvent implements Event {

    private final String stream = UUID.randomUUID().toString();
    private final String name;

    SampleEvent(String name) {
        this.name = name;
    }

    @Override
    public String stream() {
        return null;
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