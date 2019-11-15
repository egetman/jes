package store.jesframework.common;

import java.util.Collection;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import lombok.Cleanup;
import store.jesframework.Event;
import store.jesframework.provider.JdbcStoreProvider;
import store.jesframework.provider.JpaStoreProvider;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static store.jesframework.internal.Events.SampleEvent;
import static store.jesframework.internal.FancyStuff.newEntityManagerFactory;
import static store.jesframework.internal.FancyStuff.newPostgresDataSource;
import static store.jesframework.serializer.api.Format.BINARY_KRYO;
import static store.jesframework.serializer.api.Format.JSON_JACKSON;
import static store.jesframework.serializer.api.Format.XML_XSTREAM;

class FailureTest {

    @Test
    @SuppressWarnings("ConstantConditions")
    void failureShouldProtectItsInvariants() {
        final SampleEvent event = new SampleEvent("");
        assertThrows(NullPointerException.class, () -> new SagaFailure(null, "", 0, "cause"));
        assertThrows(NullPointerException.class, () -> new SagaFailure(event, null, -1, "cause"));
        assertDoesNotThrow(() -> new SagaFailure(event, "", 1, null));

        assertThrows(NullPointerException.class, () -> new ProjectionFailure(null, "", 0, "cause"));
        assertThrows(NullPointerException.class, () -> new ProjectionFailure(event, null, -1, "cause"));
        assertDoesNotThrow(() -> new ProjectionFailure(event, "", 1, null));
    }

    @Test
    void failureShouldHaveSameUuidAsSourceEvent() {
        final SampleEvent event = new SampleEvent("", UUID.randomUUID());
        final SagaFailure sagaFailure = new SagaFailure(event, "", 1, "");
        assertEquals(event.uuid(), sagaFailure.uuid());

        final ProjectionFailure projectionFailure = new ProjectionFailure(event, "", 1, "");
        assertEquals(event.uuid(), projectionFailure.uuid());
    }

    @Test
    void stringProviderShouldBeAbleToDeserializeSagaFailure() {
        @Cleanup
        final JdbcStoreProvider<String> provider = new JdbcStoreProvider<>(newPostgresDataSource(), XML_XSTREAM);

        final SampleEvent event = new SampleEvent("SampleName", UUID.randomUUID(), 100500);
        final SagaFailure failure = new SagaFailure(event, getClass().getName(), 2, "NullPointerException");

        assertDoesNotThrow(() -> provider.write(failure));

        final Collection<Event> read = provider.readBy(event.uuid());
        // just single element
        assertEquals(1, read.size());
        assertEquals(failure, read.iterator().next());
    }

    @Test
    void stringProviderShouldBeAbleToDeserializeProjectionFailure() {
        @Cleanup
        final JdbcStoreProvider<String> provider = new JdbcStoreProvider<>(newPostgresDataSource(), JSON_JACKSON);

        final SampleEvent event = new SampleEvent("SampleName", UUID.randomUUID(), 100500);
        final ProjectionFailure failure = new ProjectionFailure(event, getClass().getName(), 2, "NullPointerException");

        assertDoesNotThrow(() -> provider.write(failure));

        final Collection<Event> read = provider.readBy(event.uuid());
        // just single element
        assertEquals(1, read.size());
        assertEquals(failure, read.iterator().next());
    }

    @Test
    void byteArrayProviderShouldBeAbleToDeserializeSagaFailure() {
        @Cleanup
        final JpaStoreProvider<byte[]> provider
                = new JpaStoreProvider<>(newEntityManagerFactory(byte[].class), BINARY_KRYO);

        final SampleEvent event = new SampleEvent("SampleName", UUID.randomUUID(), 100500);
        final SagaFailure failure = new SagaFailure(event, "", 200, "ClassCastException: foo vs bar");

        assertDoesNotThrow(() -> provider.write(failure));

        final Collection<Event> read = provider.readBy(event.uuid());
        // just single element
        assertEquals(1, read.size());
        assertEquals(failure, read.iterator().next());
    }

    @Test
    void byteArrayProviderShouldBeAbleToDeserializeProjectorFailure() {
        @Cleanup
        final JpaStoreProvider<byte[]> provider
                = new JpaStoreProvider<>(newEntityManagerFactory(byte[].class), BINARY_KRYO);

        final SampleEvent event = new SampleEvent("SampleName", UUID.randomUUID(), 10500);
        final ProjectionFailure failure = new ProjectionFailure(event, "", 100, "ClassCastException: foo vs bar");

        assertDoesNotThrow(() -> provider.write(failure));

        final Collection<Event> read = provider.readBy(event.uuid());
        // just single element
        assertEquals(1, read.size());
        assertEquals(failure, read.iterator().next());
    }

}