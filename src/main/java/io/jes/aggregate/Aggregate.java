package io.jes.aggregate;

import java.util.Collection;
import java.util.UUID;
import java.util.function.Consumer;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import io.jes.Event;

public interface Aggregate {

    @Nonnull
    default UUID uuid() {
        throw new IllegalStateException("Aggregate uuid must be correctly overriden to return it's stream uuid");
    }

    @Nullable
    <T extends Event> Consumer<T> applierFor(@Nonnull Class<T> type);

    default void handleEventStream(@Nonnull Collection<Event> stream) {
        for (Event event : stream) {
            final Class<? extends Event> type = event.getClass();
            @SuppressWarnings({"unchecked", "RedundantCast"})
            final Consumer<Event> applier = (Consumer<Event>) applierFor(type);
            if (applier != null) {
                applier.accept(event);
            }
        }
    }

}
