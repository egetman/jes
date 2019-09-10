package store.jesframework.bus;

import java.util.function.Consumer;

import javax.annotation.Nonnull;

import store.jesframework.Command;

/**
 * ES Command bus. Dispatches commands across system.
 */
public interface CommandBus {

    /**
     * Dispatches command for all interested consumers.
     *
     * @param command to dispatch.
     */
    void dispatch(@Nonnull Command command);

    /**
     * Action registration.
     *
     * @param type   is type of the command.
     * @param action is action to be performed on 'type' dispatch.
     * @param <T>    class of the command.
     */
    <T extends Command> void onCommand(@Nonnull Class<T> type, @Nonnull Consumer<? super T> action);

}
