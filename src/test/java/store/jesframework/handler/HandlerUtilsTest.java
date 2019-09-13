package store.jesframework.handler;

import java.lang.reflect.Method;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;

import lombok.SneakyThrows;
import store.jesframework.Command;
import store.jesframework.ex.BrokenHandlerException;
import store.jesframework.internal.Commands;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class HandlerUtilsTest {

    @Test
    @SneakyThrows
    void failedInvocationShouldBeWrappedInBrokenHandlerException() {
        //noinspection Convert2Lambda
        final Consumer<Command> sample = new Consumer<Command>() {

            @Override
            public void accept(Command command) {
                throw new UnsupportedOperationException("Foo");
            }
        };

        final Method accept = sample.getClass().getMethod("accept", Command.class);
        final BrokenHandlerException exception = assertThrows(BrokenHandlerException.class,
                () -> HandlerUtils.invokeHandle(accept, sample, new Commands.SampleCommand("name")));

        assertEquals("Foo", exception.getMessage());
        assertEquals(UnsupportedOperationException.class, exception.getCause().getClass());
    }

}