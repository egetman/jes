package store.jesframework.handler;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import store.jesframework.AggregateStore;
import store.jesframework.Command;
import store.jesframework.JEventStore;
import store.jesframework.bus.CommandBus;
import store.jesframework.bus.SyncCommandBus;
import store.jesframework.ex.BrokenHandlerException;
import store.jesframework.internal.Commands;
import store.jesframework.provider.InMemoryStoreProvider;
import store.jesframework.provider.StoreProvider;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

class CommandHandlerTest {

    @Test
    @SuppressWarnings({"ConstantConditions", "unused"})
    void shouldHandleItsInvariants() {
        final CommandBus bus = mock(CommandBus.class);
        final JEventStore store = mock(JEventStore.class);

        assertThrows(NullPointerException.class, () -> new CommandHandler(store, null) {});
        assertThrows(NullPointerException.class, () -> new CommandHandler((JEventStore) null, bus) {});
        assertThrows(NullPointerException.class, () -> new CommandHandler((AggregateStore) null, bus) {});

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

    @Test
    void commandHandlerShouldSetEventStoreWhenInitializedWithAggregateStore() {
        final SyncCommandBus bus = new SyncCommandBus();
        final AggregateStore aggregateStore = new AggregateStore(new JEventStore(mock(StoreProvider.class)));
        final CommandHandler handler = new CommandHandler(aggregateStore, bus) {
            @Handle
            @SuppressWarnings("unused")
            void handle(Commands.SampleCommand command) {}
        };

        assertNotNull(handler.store);
        assertNotNull(handler.aggregateStore);
    }
}