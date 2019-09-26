package store.jesframework.reactors;

import java.util.Objects;
import java.util.function.Consumer;
import javax.annotation.Nonnull;

import lombok.extern.slf4j.Slf4j;
import store.jesframework.Event;
import store.jesframework.JEventStore;
import store.jesframework.common.ProjectionFailure;
import store.jesframework.lock.Lock;
import store.jesframework.offset.Offset;

@Slf4j
public abstract class Projector extends Reactor {

    private final Lock lock;

    public Projector(@Nonnull JEventStore store, @Nonnull Offset offset, @Nonnull Lock lock) {
        super(store, offset);
        this.lock = Objects.requireNonNull(lock, "Lock must not be null");
    }

    @Override
    void tailStore() {
        lock.doProtectedWrite(getKey(), super::tailStore);
    }

    @Override
    protected void accept(long offset, @Nonnull Event event, @Nonnull Consumer<? super Event> consumer) {
        try {
            super.accept(offset, event, consumer);
        } catch (Exception e) {
            log.error("Failed to project event {}", event, e);
            store.write(new ProjectionFailure(event, getKey(), offset, e.getMessage()));
        }
    }

    /**
     * Method used to fully recreate projection.
     */
    public void recreate() {
        lock.doProtectedWrite(getKey(), () -> {
            offset.reset(getKey());
            cleanUp();
        });
    }

    /**
     * This method used to clean up all the state (projection) made by this Projector.
     * Note: this method MUST NOT use any methods that are protected by {@link #lock} instance.
     */
    protected abstract void cleanUp();
}
