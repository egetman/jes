package io.jes.serializer;

import java.lang.reflect.Type;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnull;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import io.jes.ex.SerializationException;

public class GsonTypeAdapter<T> implements JsonSerializer<T>, JsonDeserializer<T> {

    private static final String DATA = "data";
    private static final String CLASS_NAME = "className";
    private final ConcurrentHashMap<String, Class<?>> cache = new ConcurrentHashMap<>();

    @Nonnull
    @Override
    public T deserialize(@Nonnull JsonElement jsonElement, @Nonnull Type type,
                         @Nonnull JsonDeserializationContext context) {
        final JsonObject jsonObject = jsonElement.getAsJsonObject();
        final JsonPrimitive primitive = (JsonPrimitive) jsonObject.get(CLASS_NAME);
        final String className = primitive.getAsString();
        final Class<?> clazz = getObjectClass(className);
        return context.deserialize(jsonObject.get(DATA), clazz);
    }

    // possible fuckup on object class rename
    @Nonnull
    @Override
    public JsonElement serialize(@Nonnull T object, @Nonnull Type type, @Nonnull JsonSerializationContext context) {
        final JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty(CLASS_NAME, object.getClass().getName());
        jsonObject.add(DATA, context.serialize(object));
        return jsonObject;
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