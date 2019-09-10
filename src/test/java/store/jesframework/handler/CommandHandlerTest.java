package store.jesframework.handler;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import store.jesframework.Command;
import store.jesframework.JEventStore;
import store.jesframework.bus.CommandBus;
import store.jesframework.bus.SyncCommandBus;
import store.jesframework.ex.BrokenHandlerException;
import store.jesframework.internal.Commands;
import store.jesframework.provider.InMemoryStoreProvider;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class CommandHandlerTest {

    @Test
    @SuppressWarnings({"ConstantConditions", "unused"})
    void shouldHandleItsInvariants() {
        final CommandBus bus = Mockito.mock(CommandBus.class);
        final JEventStore store = Mockito.mock(JEventStore.class);

        assertThrows(NullPointerException.class, () -> new CommandHandler(store, null) {});
        assertThrows(NullPointerException.class, () -> new CommandHandler(null, bus) {});

        BrokenHandlerException exception = assertThrows(BrokenHandlerException.class,
                () -> new CommandHandler(store, bus) {});

        assertEquals("Methods with @Handle annotation not found", exception.getMessage());

        exception = assertThrows(BrokenHandlerException.class, () -> new CommandHandler(store, bus) {
            @Handle
            private int handle(Command command) {
                return -1;
            }
        });

        assertEquals("@Handle method should not have any return value", exception.getMessage());

        exception = assertThrows(BrokenHandlerException.class, () -> new CommandHandler(store, bus) {
            @Handle
            private void handle(Command first, Command second) {}
        });

        assertEquals("@Handle method should have only 1 parameter", exception.getMessage());

        exception = assertThrows(BrokenHandlerException.class, () -> new CommandHandler(store, bus) {
            @Handle
            private void handle(Object object) {}
        });

        assertEquals("@Handle method parameter must be an instance of the Command class. "
                + "Found type: " + Object.class, exception.getMessage());

        assertDoesNotThrow(() -> new CommandHandler(store, bus) {
            @Handle
            private void handle(Command command) {}
        });
    }

    @Test
    void shouldPropagateErrorMessageToClient() {
        final SyncCommandBus bus = new SyncCommandBus();
        final JEventStore store = new JEventStore(new InMemoryStoreProvider());

        @SuppressWarnings("unused")
        final CommandHandler handler = new CommandHandler(store, bus) {

            @Handle
            @SuppressWarnings("unused")
            void handle(Commands.SampleCommand command) {
                throw new IllegalStateException("Test: " + command.getName());
            }
        };

        final BrokenHandlerException exception = assertThrows(BrokenHandlerException.class,
                () -> bus.dispatch(new Commands.SampleCommand("Bar")));

        Assertions.assertEquals("Test: Bar", exception.getMessage());
    }

}