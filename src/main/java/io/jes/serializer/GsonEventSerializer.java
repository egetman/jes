package io.jes.serializer;

import java.lang.reflect.Type;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnull;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import io.jes.Event;
import io.jes.ex.SerializationException;

import static java.util.Objects.requireNonNull;

class GsonEventSerializer implements EventSerializer<String> {

    private final Gson gson;
    private final ConcurrentHashMap<String, Class<?>> cache = new ConcurrentHashMap<>();

    GsonEventSerializer() {
        this.gson = new GsonBuilder().registerTypeAdapter(Event.class, new EventAdapter()).create();
    }

    public GsonEventSerializer(@Nonnull GsonBuilder gsonBuilder) {
        this.gson = requireNonNull(gsonBuilder, "Gson builder must not be null")
                .registerTypeAdapter(Event.class, new EventAdapter())
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

    private class EventAdapter implements JsonSerializer<Event>, JsonDeserializer<Event> {

        private static final String DATA = "data";
        private static final String CLASS_NAME = "className";

        @Nonnull
        @Override
        public Event deserialize(@Nonnull JsonElement jsonElement, @Nonnull Type type,
                                 @Nonnull JsonDeserializationContext context) {
            final JsonObject jsonObject = jsonElement.getAsJsonObject();
            final JsonPrimitive primitive = (JsonPrimitive) jsonObject.get(CLASS_NAME);
            final String className = primitive.getAsString();
            final Class<?> clazz = getObjectClass(className);
            return context.deserialize(jsonObject.get(DATA), clazz);
        }

        // possible fuckup on event class rename
        @Nonnull
        @Override
        public JsonElement serialize(@Nonnull Event event, @Nonnull Type type,
                                     @Nonnull JsonSerializationContext context) {
            final JsonObject object = new JsonObject();
            object.addProperty(CLASS_NAME, event.getClass().getName());
            object.add(DATA, context.serialize(event));
            return object;
        }

        @Nonnull
        private Class<?> getObjectClass(@Nonnull String className) {
            return cache.computeIfAbsent(className, candidate -> {
                try {
                    return Class.forName(candidate);
                } catch (ClassNotFoundException e) {
                    throw new SerializationException(e);
                }
            });
        }
    }
}
