package io.jes.reactors;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Set;
import javax.annotation.Nonnull;

import io.jes.Event;
import io.jes.ex.BrokenReactorException;
import io.jes.util.Check;

class ReactorUtils {

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
        if (method.getParameterCount() != 1 ) {
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
            throw new BrokenReactorException("Reactor failure: " + e.getMessage(), e);
        }
    }

}
