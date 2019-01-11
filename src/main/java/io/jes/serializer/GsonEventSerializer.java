package io.jes.serializer;

import javax.annotation.Nonnull;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import io.jes.Event;
import io.jes.ex.SerializationException;

import static java.util.Objects.requireNonNull;

class GsonEventSerializer implements EventSerializer<String> {

    private final Gson gson;

    GsonEventSerializer() {
        this.gson = new GsonBuilder().registerTypeAdapter(Event.class, new GsonTypeAdapter<>()).create();
    }

    GsonEventSerializer(@Nonnull GsonBuilder gsonBuilder) {
        this.gson = requireNonNull(gsonBuilder, "Gson builder must not be null")
                .registerTypeAdapter(Event.class, new GsonTypeAdapter<>())
                .create();
    }

    @Override
    public String serialize(Event event) {
        try {
            return gson.toJson(event, Event.class);
        } catch (Exception e) {
            throw new SerializationException(e);
        }
    }

    @Override
    public Event deserialize(String event) {
        try {
            return gson.fromJson(event, Event.class);
        } catch (Exception e) {
            throw new SerializationException(e);
        }
    }
}
