package io.jes.snapshot;

import org.junit.jupiter.api.Test;

import io.jes.Aggregate;
import io.jes.AggregateImpl;
import io.jes.ex.AggregateCreationException;
import io.jes.internal.FancyAggregate;

import static java.util.UUID.randomUUID;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SnapshotProviderTest {

    @Test
    void shouldThrowAggregateCreationExceptionIfFailedToInstantiateAggregate() {
        final SnapshotProvider provider = new SnapshotProvider() {};

        class FooAggregate extends AggregateImpl {
            @SuppressWarnings("unused")
            public FooAggregate(String string, int integer) {
                // do nothing, just to disable no-arg constructor
            }
        }
        assertThrows(AggregateCreationException.class, () -> provider.initialStateOf(randomUUID(), FooAggregate.class));
    }

    @Test
    void shouldReportNullSnapshotValues() {
        final SnapshotProvider provider = new SnapshotProvider() {};
        //noinspection ConstantConditions
        assertThrows(NullPointerException.class, () -> provider.snapshot(null));
    }

    @Test
    void shouldReturnTheSameAggregateAsSnapshotByDefault() {
        final SnapshotProvider provider = new SnapshotProvider() {};
        Aggregate aggregate = new FancyAggregate();
        assertSame(aggregate, provider.snapshot(aggregate));
    }

}