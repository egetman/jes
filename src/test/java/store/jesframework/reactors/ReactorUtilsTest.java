package store.jesframework.reactors;

import java.lang.reflect.Method;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;

import lombok.SneakyThrows;
import store.jesframework.Event;
import store.jesframework.ex.BrokenReactorException;
import store.jesframework.internal.Events;

import static org.junit.jupiter.api.Assertions.*;

class ReactorUtilsTest {

    @Test
    @SneakyThrows
    void failedInvocationShouldBeWrappedInBrokenReactorException() {
        //noinspection Convert2Lambda
        final Consumer<Event> sample = new Consumer<Event>() {

            @Override
            public void accept(Event event) {
                throw new UnsupportedOperationException("Boom");
            }
        };

        final Method accept = sample.getClass().getMethod("accept", Event.class);
        final Events.SampleEvent event = new Events.SampleEvent("");
        final BrokenReactorException exception = assertThrows(BrokenReactorException.class,
                () -> ReactorUtils.invokeReactsOn(accept, sample, event));

        assertEquals("Boom", exception.getMessage());
        assertEquals(UnsupportedOperationException.class, exception.getCause().getClass());
    }

}