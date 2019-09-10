package store.jesframework.bus;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import store.jesframework.Command;

class NoopCommandBusTest {

    @Test
    void noopBusDispatchShouldNotThrowExceptions() {
        final NoopCommandBus commandBus = new NoopCommandBus();
        Assertions.assertDoesNotThrow(() -> commandBus.dispatch(null));
        Assertions.assertDoesNotThrow(() -> commandBus.onCommand(null, command -> {}));
        Assertions.assertDoesNotThrow(() -> commandBus.onCommand(Command.class, null));
    }

    @Test
    void noopBusShouldNotReactOnDispatch() {
        final Command command = new Command() {};

        final CommandBus bus = new NoopCommandBus();
        bus.onCommand(command.getClass(), target -> {
            throw new IllegalStateException("Boom");
        });
        Assertions.assertDoesNotThrow(() -> bus.dispatch(command));
    }

}