package store.jesframework.bus;

import java.util.function.Consumer;
import javax.annotation.Nullable;

import store.jesframework.Command;

/**
 * Empty implementation (no operation).
 */
public class NoopCommandBus implements CommandBus {

    @Override
    public void dispatch(@Nullable Command command) {
        // nothing to do
    }

    @Override
    public <T extends Command> void onCommand(@Nullable Class<T> type, @Nullable Consumer<? super T> action) {
        // noting to do
    }
}
