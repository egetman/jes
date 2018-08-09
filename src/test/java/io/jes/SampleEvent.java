package io.jes;

import java.util.UUID;

class SampleEvent implements Event {

    private long id;
    private final String stream = UUID.randomUUID().toString();
    private final String name;

    SampleEvent(String name) {
        this.name = name;
    }

    @Override
    public long getId() {
        return id;
    }

    @Override
    public void setId(long id) {
        this.id = id;
    }

    @Override
    public String stream() {
        return stream;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder(getClass().getSimpleName() + " [");
        sb.append("id: ").append(id);
        sb.append(", name: ").append(name);
        sb.append(", stream: ").append(stream());
        sb.append(']');
        return sb.toString();
    }
}