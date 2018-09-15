package io.jes;

import java.util.function.UnaryOperator;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

@SuppressWarnings("unused")
public interface JEventStore {

    /**
     * TODO:
     *
     * A Stream potentially wraps underlying data store-specific resources and must, therefore, be closed after usage.
     * You can either manually close the Stream by using the close() method or by using a Java 7 try-with-resources
     * block, as shown in the following example:
     * <code>
     *
     * <p>
     * try (Stream{@code <}User{@code >} stream = store.readFrom(0)) {
     *      stream.forEach(…);
     * }
     * </code>
     *
     * @param offset the offset to read from.
     * @return {@link Stream} of events stored in that {@literal EventStore}.
     */
    Stream<Event> readFrom(long offset);

    /**
     * TODO:
     *
     * A Stream potentially wraps underlying data store-specific resources and must, therefore, be closed after usage.
     * You can either manually close the Stream by using the close() method or by using a Java 7 try-with-resources
     * block, as shown in the following example:
     * <code>
     *
     * <p>
     * try (Stream{@code <}User{@code >} stream = store.readFrom(0)) {
     *      stream.forEach(…);
     * }
     * </code>
     *
     * @param stream the event stream to read. The {@literal stream} action as common (or group) identifier.
     * @return {@link Stream} of events stored in that {@literal EventStore}.
     */
    Stream<Event> readBy(@Nonnull String stream);

    void write(@Nonnull Event event);

    void copyTo(JEventStore store);

    void copyTo(JEventStore store, UnaryOperator<Event> handler);

}
