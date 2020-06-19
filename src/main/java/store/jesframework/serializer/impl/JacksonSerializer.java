package store.jesframework.serializer.impl;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DatabindContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.TypeIdResolver;
import com.fasterxml.jackson.databind.jsontype.impl.LaissezFaireSubTypeValidator;
import com.fasterxml.jackson.databind.jsontype.impl.TypeIdResolverBase;
import com.fasterxml.jackson.databind.type.TypeFactory;

import lombok.extern.slf4j.Slf4j;
import store.jesframework.ex.SerializationException;
import store.jesframework.serializer.api.Format;
import store.jesframework.serializer.api.Serializer;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.ANY;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;
import static com.fasterxml.jackson.annotation.JsonTypeInfo.As;
import static com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import static com.fasterxml.jackson.databind.DeserializationFeature.ADJUST_DATES_TO_CONTEXT_TIME_ZONE;
import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;
import static com.fasterxml.jackson.databind.ObjectMapper.DefaultTypeResolverBuilder;
import static com.fasterxml.jackson.databind.ObjectMapper.DefaultTyping.NON_FINAL;
import static com.fasterxml.jackson.databind.SerializationFeature.FAIL_ON_EMPTY_BEANS;
import static com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATES_AS_TIMESTAMPS;

@Slf4j
class JacksonSerializer<S> implements Serializer<S, String> {

    /**
     * A little hack to faster resolve type name. All json (jackson) events (for now) starts with {"@type":"
     */
    private static final int START_TYPE_NAME_POSITION = 10;
    static final int DEFAULT_NAME_SIZE = 60;

    private final ObjectMapper mapper;
    private final TypeReference<S> serializationType = new TypeReference<S>() {};

    JacksonSerializer() {
        this(Context.parse());
    }

    JacksonSerializer(@Nonnull Context<?> context) {
        this(new ObjectMapper(), context);
    }

    JacksonSerializer(@Nonnull ObjectMapper mapper, @Nonnull Context<?> context) {
        this.mapper = Objects.requireNonNull(mapper, "ObjectMapper must not be null");
        Objects.requireNonNull(context, "Context must not be null");
        configureMapper(this.mapper, context);
    }

    private void configureMapper(@Nonnull ObjectMapper mapper, @Nonnull Context<?> context) {
        mapper.disable(FAIL_ON_EMPTY_BEANS);
        mapper.disable(FAIL_ON_UNKNOWN_PROPERTIES);
        mapper.disable(WRITE_DATES_AS_TIMESTAMPS);
        mapper.disable(ADJUST_DATES_TO_CONTEXT_TIME_ZONE);
        mapper.setSerializationInclusion(NON_NULL);

        final TypeIdResolver resolver = new TypeIdWithClassNameFallbackResolver(context);

        mapper.setDefaultTyping(new DefaultTypeResolverBuilder(NON_FINAL, LaissezFaireSubTypeValidator.instance)
                .init(Id.CUSTOM, resolver)
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

    @Nullable
    @Override
    public String fetchTypeName(@Nonnull String raw) {
        Objects.requireNonNull(raw, "The raw event must not be null");
        int size = 0;
        final char[] searched = raw.toCharArray();
        char[] typeNameArray = new char[DEFAULT_NAME_SIZE];

        for (int i = START_TYPE_NAME_POSITION; i < searched.length; i++) {
            if (searched[i] == '"') {
                break;
            }
            typeNameArray[size++] = searched[i];
            if (size == typeNameArray.length) {
                final char[] temp = new char[typeNameArray.length * 2];
                System.arraycopy(typeNameArray, 0, temp, 0, typeNameArray.length);
                typeNameArray = temp;
            }
        }
        return new String(typeNameArray, 0, size);
    }

    @Nonnull
    @Override
    public Format format() {
        return Format.JSON_JACKSON;
    }

    private static class TypeIdWithClassNameFallbackResolver extends TypeIdResolverBase {

        private final Map<Class<?>, String> serializationAliases;
        private final Map<String, Class<?>> deserializationAliases;
        // concurrent access at runtime
        private final Map<Class<?>, JavaType> typeCache = new ConcurrentHashMap<>();

        TypeIdWithClassNameFallbackResolver(@Nonnull Context<?> context) {
            this.serializationAliases = context.classesToAliases();
            this.deserializationAliases = context.aliasesToClasses();
            log.debug("Prepared {} type alias(es)", serializationAliases.size());
        }

        @Override
        public String idFromValue(Object value) {
            return idFromValueAndType(value, value != null ? value.getClass() : null);
        }

        @Override
        public String idFromValueAndType(@Nullable Object value, @Nullable Class<?> suggestedType) {
            final String alias = serializationAliases.get(suggestedType);
            // if alias not registered, store info about class name
            if (alias != null) {
                return alias;
            }
            return suggestedType != null ? suggestedType.getName() : "Unknown Type";
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
            return typeCache.computeIfAbsent(clazz, key -> TypeFactory.defaultInstance().constructType(key));
        }

        @Override
        public Id getMechanism() {
            return Id.CUSTOM;
        }
    }

}
