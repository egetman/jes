package io.jes.handler;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Set;
import javax.annotation.Nonnull;

import io.jes.Command;
import io.jes.ex.BrokenHandlerException;
import io.jes.util.Check;

class HandlerUtils {

    private HandlerUtils() {}

    @Nonnull
    static Set<Method> getAllHandleMethods(@Nonnull Class<?> source) {
        final Set<Method> methods = new HashSet<>();
        final Method[] sourceMethods = source.getDeclaredMethods();
        for (Method sourceMethod : sourceMethods) {
            if (sourceMethod.isAnnotationPresent(Handle.class) && !sourceMethod.isSynthetic()) {
                methods.add(sourceMethod);
            }
        }
        Check.nonEmpty(methods, () -> new BrokenHandlerException("Methods with @Handle annotation not found"));
        return methods;
    }

    static void ensureHandleHasOneParameter(@Nonnull Method method) {
        if (method.getParameterCount() != 1 ) {
            throw new BrokenHandlerException("@Handle method should have only 1 parameter");
        }
    }

    static void ensureHandleHasVoidReturnType(@Nonnull Method method) {
        if (!method.getReturnType().equals(Void.TYPE)) {
            throw new BrokenHandlerException("@Handle method should not have any return value");
        }
    }

    static void ensureHandleHasEventParameter(@Nonnull Method method) {
        if (!Command.class.isAssignableFrom(method.getParameterTypes()[0])) {
            throw new BrokenHandlerException("@Handle method parameter must be an instance of the Command class. "
                    + "Found type: " + method.getParameterTypes()[0]);
        }
    }

    static void invokeHandle(@Nonnull Method method, @Nonnull Object source, @Nonnull Command command) {
        try {
            method.invoke(source, command);
        } catch (IllegalAccessException | InvocationTargetException e) {
            Throwable cause = e.getCause();
            while (cause.getCause() != null) {
                cause = cause.getCause();
            }
            throw new BrokenHandlerException(cause.getMessage(), cause);
        }
    }

}
