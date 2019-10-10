package store.jesframework.serializer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DatabindContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.impl.TypeIdResolverBase;
import com.fasterxml.jackson.databind.type.TypeFactory;

import store.jesframework.ex.SerializationException;
import store.jesframework.serializer.api.Serializer;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.ANY;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;
import static com.fasterxml.jackson.annotation.JsonTypeInfo.As;
import static com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import static com.fasterxml.jackson.databind.DeserializationFeature.ADJUST_DATES_TO_CONTEXT_TIME_ZONE;
import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;
import static com.fasterxml.jackson.databind.ObjectMapper.DefaultTypeResolverBuilder;
import static com.fasterxml.jackson.databind.ObjectMapper.DefaultTyping;
import static com.fasterxml.jackson.databind.SerializationFeature.FAIL_ON_EMPTY_BEANS;
import static com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATES_AS_TIMESTAMPS;

class JacksonSerializer<S> implements Serializer<S, String> {

    private final ObjectMapper mapper;
    private final TypeReference<S> serializationType = new TypeReference<S>() {};

    @SuppressWarnings("WeakerAccess")
    public JacksonSerializer() {
        this(null);
    }

    @SuppressWarnings("WeakerAccess")
    public JacksonSerializer(@Nullable TypeRegistry registry) {
        this(new ObjectMapper(), registry);
    }

    @SuppressWarnings("WeakerAccess")
    public JacksonSerializer(@Nonnull ObjectMapper mapper, @Nullable TypeRegistry registry) {
        this.mapper = Objects.requireNonNull(mapper, "ObjectMapper must not be null");
        configureMapper(this.mapper, registry);
    }

    private void configureMapper(@Nonnull ObjectMapper mapper, @Nullable TypeRegistry registry) {
        mapper.disable(FAIL_ON_EMPTY_BEANS);
        mapper.disable(FAIL_ON_UNKNOWN_PROPERTIES);
        mapper.disable(WRITE_DATES_AS_TIMESTAMPS);
        mapper.disable(ADJUST_DATES_TO_CONTEXT_TIME_ZONE);
        mapper.setSerializationInclusion(NON_NULL);

        final Map<Class<?>, String> aliases = registry == null ? new HashMap<>() : registry.getAliases();
        final Map<String, Class<?>> reversed = new HashMap<>();
        for (Map.Entry<Class<?>, String> entry : aliases.entrySet()) {
            reversed.put(entry.getValue(), entry.getKey());
        }

        mapper.setDefaultTyping(new DefaultTypeResolverBuilder(DefaultTyping.NON_FINAL)
                .init(Id.CUSTOM, new TypeIdWithClassNameFallbackResolver(aliases, reversed))
                .inclusion(As.PROPERTY)
                .typeProperty("@type"));
        mapper.setVisibility(mapper.getSerializationConfig().getDefaultVisibilityChecker()
                .withFieldVisibility(ANY)
                .withGetterVisibility(NONE)
                .withSetterVisibility(NONE)
                .withCreatorVisibility(NONE));
        mapper.findAndRegisterModules();
    }

    @Nonnull
    @Override
    public String serialize(@Nonnull S toSerialize) {
        try {
            return mapper.writeValueAsString(toSerialize);
        } catch (Exception e) {
            throw new SerializationException(e);
        }
    }

    @Nonnull
    @Override
    public S deserialize(@Nonnull String toDeserialize) {
        try {
            return mapper.readValue(toDeserialize, serializationType);
        } catch (IOException e) {
            throw new SerializationException(e);
        }
    }

    private static class TypeIdWithClassNameFallbackResolver extends TypeIdResolverBase {

        private final Map<Class<?>, String> serializationAliases;
        private final Map<String, Class<?>> deserializationAliases;
        private final Map<Class<?>, JavaType> typesCache = new ConcurrentHashMap<>();

        TypeIdWithClassNameFallbackResolver(@Nullable Map<Class<?>, String> serializationAliases,
                                            @Nullable Map<String, Class<?>> deserializationAliases) {
            this.serializationAliases = getNonNullOrDefault(serializationAliases, new HashMap<>());
            this.deserializationAliases = getNonNullOrDefault(deserializationAliases, new HashMap<>());
        }

        private <K, V> Map<K, V> getNonNullOrDefault(@Nullable Map<K, V> map, @Nonnull Map<K, V> defaultValue) {
            return map != null ? map : Objects.requireNonNull(defaultValue, "Default value must not be null");
        }

        @Override
        public String idFromValue(Object value) {
            return idFromValueAndType(value, value != null ? value.getClass() : null);
        }

        @Override
        public String idFromValueAndType(Object value, Class<?> suggestedType) {
            final String alias = serializationAliases.get(suggestedType);
            // if alias not registered, store info about class name
            return alias != null ? alias : suggestedType.getName();
        }

        @Override
        public JavaType typeFromId(DatabindContext context, String id) {
            Class<?> clazz = deserializationAliases.get(id);
            if (clazz == null) {
                // fallback to resolving type by class name
                try {
                    clazz = Class.forName(id);
                } catch (ClassNotFoundException e) {
                    throw new TypeNotPresentException(id, e);
                }
            }
            return typesCache.computeIfAbsent(clazz, key -> TypeFactory.defaultInstance().constructType(key));
        }

        @Override
        public Id getMechanism() {
            return Id.CUSTOM;
        }
    }

}
