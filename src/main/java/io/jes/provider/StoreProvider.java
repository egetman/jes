package io.jes.provider;

import java.util.stream.Stream;
import javax.annotation.Nonnull;

import io.jes.Event;

public interface StoreProvider {

    Stream<Event> readFrom(long offset);

    Stream<Event> readBy(@Nonnull String stream);

    void write(@Nonnull Event event);

}
