package store.jesframework.common;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import lombok.Cleanup;
import store.jesframework.Event;
import store.jesframework.provider.JdbcStoreProvider;
import store.jesframework.provider.JpaStoreProvider;

import static java.time.LocalDateTime.now;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static store.jesframework.internal.Events.SampleEvent;
import static store.jesframework.internal.FancyStuff.newEntityManagerFactory;
import static store.jesframework.internal.FancyStuff.newPostgresDataSource;

class HandlingFailureTest {

    @Test
    @SuppressWarnings("ConstantConditions")
    void handlingFailureShouldProtectItsInvariants() {
        final SampleEvent event = new SampleEvent("");
        assertThrows(NullPointerException.class, () -> new HandlingFailure(null, now(), "", 0));
        assertThrows(NullPointerException.class, () -> new HandlingFailure(event, null, "", -1));
        assertThrows(NullPointerException.class, () -> new HandlingFailure(event, now(), null, 1));
    }

    @Test
    void handlingFailureShouldHaveSameUuidAsSourceEvent() {
        final SampleEvent event = new SampleEvent("", UUID.randomUUID());
        final HandlingFailure failure = new HandlingFailure(event, now(), "", 1);
        assertEquals(event.uuid(), failure.uuid());
    }

    @Test
    void stringProviderShouldBeAbleToDeserializeHandlingFailure() {
        @Cleanup
        final JdbcStoreProvider<String> provider = new JdbcStoreProvider<>(newPostgresDataSource(), String.class);

        final SampleEvent event = new SampleEvent("SampleName", UUID.randomUUID(), 100500);
        final HandlingFailure failure = new HandlingFailure(event, now(), getClass().getName(), 2);

        assertDoesNotThrow(() -> provider.write(failure));

        final Collection<Event> read = provider.readBy(event.uuid());
        // just single element
        assertEquals(1, read.size());
        assertEquals(failure, read.iterator().next());
    }

    @Test
    void byteArrayProviderShouldBeAbleToDeserializeHandlingFailure() {
        @Cleanup
        final JpaStoreProvider<byte[]> provider
                = new JpaStoreProvider<>(newEntityManagerFactory(byte[].class), byte[].class);

        final SampleEvent event = new SampleEvent("SampleName", UUID.randomUUID(), 100500);
        final HandlingFailure failure = new HandlingFailure(event, LocalDateTime.MIN, "", 200);

        assertDoesNotThrow(() -> provider.write(failure));

        final Collection<Event> read = provider.readBy(event.uuid());
        // just single element
        assertEquals(1, read.size());
        assertEquals(failure, read.iterator().next());
    }

}