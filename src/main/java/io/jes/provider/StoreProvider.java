package io.jes.provider;

import java.util.Collection;
import java.util.UUID;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

import io.jes.Event;

public interface StoreProvider {

    Stream<Event> readFrom(long offset);

    Collection<Event> readBy(@Nonnull UUID uuid);

    void write(@Nonnull Event event);

}
