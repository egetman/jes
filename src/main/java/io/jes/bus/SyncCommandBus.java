package io.jes.bus;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import javax.annotation.Nonnull;

import io.jes.Command;
import lombok.extern.slf4j.Slf4j;

import static java.util.Objects.requireNonNull;

@Slf4j
public class SyncCommandBus implements CommandBus {

    private final Map<Class<? extends Command>, Collection<Consumer<? super Command>>> endpoints =
            new ConcurrentHashMap<>();

    @Override
    public void dispatch(@Nonnull Command command) {
        final Collection<Consumer<? super Command>> consumers = endpoints.get(command.getClass());
        if (consumers != null) {
            consumers.forEach(consumer -> consumer.accept(command));
        }
    }

    @Override
    public <T extends Command> void onCommand(@Nonnull Class<T> type, @Nonnull Consumer<? super T> action) {
        endpoints.putIfAbsent(requireNonNull(type, "Type must not be null"), new CopyOnWriteArrayList<>());
        //noinspection unchecked
        endpoints.get(type).add(requireNonNull((Consumer<? super Command>) action));
        log.debug("Command {} registered", type.getName());
    }
}
