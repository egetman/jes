package io.jes.serializer;

import javax.annotation.Nonnull;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import io.jes.Aggregate;
import io.jes.ex.SerializationException;

import static java.util.Objects.requireNonNull;

public class GsonAggregateSerializer implements AggregateSerializer<String> {

    private final Gson gson;

    GsonAggregateSerializer() {
        this.gson = new GsonBuilder().registerTypeAdapter(Aggregate.class, new GsonTypeAdapter<>()).create();
    }

    @SuppressWarnings("unused")
    GsonAggregateSerializer(@Nonnull GsonBuilder gsonBuilder) {
        this.gson = requireNonNull(gsonBuilder, "Gson builder must not be null")
                .registerTypeAdapter(Aggregate.class, new GsonTypeAdapter<>())
                .create();
    }

    @Override
    public String serialize(Aggregate aggregate) {
        try {
            return gson.toJson(aggregate, Aggregate.class);
        } catch (Exception e) {
            throw new SerializationException(e);
        }
    }

    @Override
    public Aggregate deserialize(String aggregate) {
        try {
            return gson.fromJson(aggregate, Aggregate.class);
        } catch (Exception e) {
            throw new SerializationException(e);
        }
    }
}
