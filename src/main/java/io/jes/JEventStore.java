package io.jes;

import java.util.Collection;
import javax.annotation.Nonnull;

public interface JEventStore {

    Collection<Event> readFrom(long offset);

    Collection<Event> readAllFor(@Nonnull Link link);

    void write(@Nonnull Event event, @Nonnull Link link);

}
