package store.jesframework.bus;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import store.jesframework.Command;
import lombok.SneakyThrows;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SyncCommandBusTest {

    @Test
    @SuppressWarnings("ConstantConditions")
    void shouldHandleInvariants() {
        final CommandBus bus = new SyncCommandBus();
        assertThrows(NullPointerException.class, () -> bus.dispatch(null));
        assertThrows(NullPointerException.class, () -> bus.onCommand(null, command -> {}));
        assertThrows(NullPointerException.class, () -> bus.onCommand(Command.class, null));
    }

    @Test
    void busShouldIgnoreUnregisteredCommands() {
        final CommandBus bus = new SyncCommandBus();
        assertDoesNotThrow(() -> bus.dispatch(new Command() {}));
    }

    @Test
    @SneakyThrows
    void busShouldReactOnRegisteredCommands() {
        final Command command = new Command() {};
        final CountDownLatch latch = new CountDownLatch(1);

        final CommandBus bus = new SyncCommandBus();
        bus.onCommand(command.getClass(), target -> latch.countDown());
        bus.dispatch(command);
        assertTrue(latch.await(1, TimeUnit.SECONDS));
    }

}