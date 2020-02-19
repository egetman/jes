package store.jesframework.serializer.impl;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.reflections.Reflections;
import org.reflections.util.ConfigurationBuilder;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import store.jesframework.Aggregate;
import store.jesframework.Event;
import store.jesframework.serializer.api.AliasingStrategy;
import store.jesframework.serializer.api.Format;
import store.jesframework.serializer.api.SerializationOption;
import store.jesframework.serializer.api.TypeAlias;
import store.jesframework.serializer.api.Upcaster;

import static java.util.Objects.requireNonNull;
import static org.reflections.util.ClasspathHelper.forPackage;

/**
 * Aggregated meta information about serialization stuff.
 *
 * @param <T> is a raw serialized type.
 */
// todo: cache lookup results? It called just at init time. (subtypes of Event & Aggregate classes)
@Slf4j
@SuppressWarnings("squid:S1135")
public class Context<T> {

    // safe defaults
    @Getter(AccessLevel.PACKAGE)
    private Format format = Format.JSON_JACKSON;
    private AliasingStrategy aliasingStrategy = AliasingStrategy.FULL_CLASS_NAME;

    // no need of concurrent one
    private final Collection<TypeAlias> aliases = new HashSet<>();
    private final Map<String, Upcaster<T>> upcasters = new HashMap<>();

    // try scan from the base packages
    private final Reflections reflections =
            new Reflections(new ConfigurationBuilder().addUrls(forPackage("")).setExpandSuperTypes(false));

    Context() {
    }

    // no need to try fetch event type/name if there is not upcasters at all
    boolean isUpcastingEnabled() {
        return !upcasters.isEmpty();
    }


    @Nonnull
    T tryUpcast(@Nonnull T raw, @Nullable String typeName) {
        if (upcasters.isEmpty() || typeName == null) {
            return raw;
        }
        log.trace("Resolved event type {}", typeName);
        final Upcaster<T> upcaster = upcasters.get(typeName);
        if (upcaster != null) {
            try {
                return requireNonNull(upcaster.upcast(raw), "Upcaster must not return null");
            } catch (Exception e) {
                log.error("Failed to upcast raw type: {}", raw, e);
            }
        }
        return raw;
    }

    // called only during system setup.
    private void addUpcaster(@Nonnull Upcaster<T> upcaster) {
        requireNonNull(upcaster, "Upcaster must not be null");
        upcasters.put(requireNonNull(upcaster.eventTypeName(), "EventTypeName must not be null"), upcaster);
    }

    @Nonnull
    Map<Class<?>, String> classesToAliases() {
        final Map<Class<?>, String> classesToAliases = new HashMap<>();
        // first: write all common aliases
        if (aliasingStrategy == AliasingStrategy.SHORT_CLASS_NAME) {
            for (Class<? extends Event> type : findAllEventTypes()) {
                classesToAliases.put(type, type.getSimpleName());
            }
            for (Class<? extends Aggregate> type : findAllAggregateTypes()) {
                classesToAliases.put(type, type.getSimpleName());
            }
        }
        // second: write provided custom aliases, overriding defaults if necessary
        for (TypeAlias alias : aliases) {
            classesToAliases.put(alias.getType(), alias.getAlias());
        }
        return classesToAliases;
    }

    @Nonnull
    // todo: allow adding multiple aliases to single class.
    Map<String, Class<?>> aliasesToClasses() {
        final Map<String, Class<?>> aliasesToClasses = new HashMap<>();
        // first: write all common aliases
        if (aliasingStrategy == AliasingStrategy.SHORT_CLASS_NAME) {
            for (Class<? extends Event> type : findAllEventTypes()) {
                aliasesToClasses.put(type.getSimpleName(), type);
            }
            for (Class<? extends Aggregate> type : findAllAggregateTypes()) {
                aliasesToClasses.put(type.getSimpleName(), type);
            }
        }
        // second: write provided custom aliases, overriding defaults if necessary
        for (TypeAlias alias : aliases) {
            aliasesToClasses.put(alias.getAlias(), alias.getType());
        }
        return aliasesToClasses;
    }

    /**
     * Try to find all subtypes of {@link store.jesframework.Event} class.
     */
    private Set<Class<? extends Event>> findAllEventTypes() {
        return reflections.getSubTypesOf(Event.class);
    }

    /**
     * Try to find all subtypes of {@link store.jesframework.Aggregate} class.
     */
    private Set<Class<? extends Aggregate>> findAllAggregateTypes() {
        return reflections.getSubTypesOf(Aggregate.class);
    }

    static <T> Context<T> empty() {
        return Context.parse();
    }

    static <T> Context<T> parse(@Nullable SerializationOption... options) {
        final Context<T> context = new Context<>();
        if (options == null) {
            return context;
        }

        for (SerializationOption option : options) {
            if (option instanceof TypeAlias) {
                context.aliases.add((TypeAlias) option);
            } else if (option instanceof Format) {
                context.format = (Format) option;
            } else if (option instanceof AliasingStrategy) {
                context.aliasingStrategy = (AliasingStrategy) option;
            } else if (option instanceof Upcaster) {
                try {
                    //noinspection unchecked
                    context.addUpcaster((Upcaster<T>) option);
                } catch (ClassCastException e) {
                    log.warn("Failed to register upcaster {}: type mismatch", option, e);
                }
            } else {
                log.warn("Unsupported serialization option found: {}", option);
            }
        }
        log.debug("parsing of {} serialization option(s) complete", options.length);
        return context;
    }

}
