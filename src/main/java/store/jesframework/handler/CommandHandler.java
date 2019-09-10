package store.jesframework.handler;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import javax.annotation.Nonnull;

import store.jesframework.Command;
import store.jesframework.JEventStore;
import store.jesframework.bus.CommandBus;
import lombok.extern.slf4j.Slf4j;

import static store.jesframework.handler.HandlerUtils.ensureHandleHasEventParameter;
import static store.jesframework.handler.HandlerUtils.ensureHandleHasOneParameter;
import static store.jesframework.handler.HandlerUtils.ensureHandleHasVoidReturnType;
import static store.jesframework.handler.HandlerUtils.getAllHandleMethods;
import static store.jesframework.handler.HandlerUtils.invokeHandle;

@Slf4j
public abstract class CommandHandler {

    protected final JEventStore store;

    public CommandHandler(@Nonnull JEventStore store, @Nonnull CommandBus bus) {
        Objects.requireNonNull(bus, "Command bus must not be null");
        this.store = Objects.requireNonNull(store, "Event store must not be null");
        // read all configured handlers for each CommandHandler
        final Map<Class<? extends Command>, Consumer<? super Command>> handlers = readHandlers();
        for (Map.Entry<Class<? extends Command>, Consumer<? super Command>> entry : handlers.entrySet()) {
            bus.onCommand(entry.getKey(), entry.getValue());
        }
        log.debug("{} initialized", getClass().getName());
    }

    @Nonnull
    private Map<Class<? extends Command>, Consumer<? super Command>> readHandlers() {
        final Set<Method> methods = getAllHandleMethods(getClass());
        log.debug("Resolved {} handler methods", methods.size());
        final Map<Class<? extends Command>, Consumer<? super Command>> commandToConsumer = new HashMap<>();
        for (Method method : methods) {
            log.debug("Start verification of '{}'", method);
            ensureHandleHasOneParameter(method);
            ensureHandleHasVoidReturnType(method);
            ensureHandleHasEventParameter(method);
            log.debug("Verification of '{}' complete", method);

            method.setAccessible(true);
            @SuppressWarnings("unchecked")
            final Class<? extends Command> commandType = (Class<? extends Command>) method.getParameterTypes()[0];
            commandToConsumer.put(commandType, (command -> invokeHandle(method, this, command)));
        }

        return commandToConsumer;
    }

}
