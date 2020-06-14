package store.jesframework.reactors;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import javax.annotation.Nonnull;

import com.fasterxml.uuid.impl.NameBasedGenerator;

import lombok.extern.slf4j.Slf4j;
import store.jesframework.Event;
import store.jesframework.ex.BrokenReactorException;
import store.jesframework.util.Check;

import static com.fasterxml.uuid.Generators.nameBasedGenerator;
import static com.fasterxml.uuid.impl.NameBasedGenerator.NAMESPACE_DNS;
import static java.security.MessageDigest.getInstance;

@Slf4j
class ReactorUtils {

    private static final NameBasedGenerator GENERATOR;

    static {
        try {
            GENERATOR = nameBasedGenerator(NAMESPACE_DNS, getInstance("SHA-1"));
        } catch (NoSuchAlgorithmException e) {
            log.error("Failed to find sha-1 impl: ", e);
            throw new IllegalStateException(e);
        }
    }

    private ReactorUtils() {}

    @Nonnull
    static Set<Method> getAllReactsOnMethods(@Nonnull Class<?> source) {
        final Set<Method> methods = new HashSet<>();
        final Method[] sourceMethods = source.getDeclaredMethods();
        for (Method sourceMethod : sourceMethods) {
            if (sourceMethod.isAnnotationPresent(ReactsOn.class) && !sourceMethod.isSynthetic()) {
                methods.add(sourceMethod);
            }
        }
        Check.nonEmpty(methods, () -> new BrokenReactorException("Methods with @ReactsOn annotation not found"));
        return methods;
    }

    static void ensureReactsOnHasOneParameter(@Nonnull Method method) {
        if (method.getParameterCount() != 1) {
            throw new BrokenReactorException("@ReactsOn method should have only 1 parameter");
        }
    }

    static void ensureReactsOnHasVoidReturnType(@Nonnull Method method) {
        if (!method.getReturnType().equals(Void.TYPE)) {
            throw new BrokenReactorException("@ReactsOn method should not have any return value");
        }
    }

    static void ensureReactsOnHasEventParameter(@Nonnull Method method) {
        if (!Event.class.isAssignableFrom(method.getParameterTypes()[0])) {
            throw new BrokenReactorException("@ReactsOn method parameter must be an instance of the Event class. "
                    + "Found type: " + method.getParameterTypes()[0]);
        }
    }

    static void invokeReactsOn(@Nonnull Method method, @Nonnull Object source, @Nonnull Event event) {
        try {
            method.invoke(source, event);
        } catch (IllegalAccessException | InvocationTargetException e) {
            Throwable cause = e.getCause();
            while (cause.getCause() != null) {
                cause = cause.getCause();
            }
            throw new BrokenReactorException(cause.getMessage(), cause);
        }
    }

    /**
     * Method for generating name-based UUIDs using specified name (serialized to bytes using UTF-8 encoding).
     */
    static UUID uuidByKey(@Nonnull String key) {
        Objects.requireNonNull(key, "Key must not be null");
        return GENERATOR.generate(key);
    }
}
