package store.jesframework.snapshot;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;

import lombok.SneakyThrows;
import store.jesframework.Aggregate;
import store.jesframework.Event;
import store.jesframework.internal.FancyAggregate;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singleton;
import static java.util.UUID.randomUUID;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;
import static store.jesframework.internal.Events.FancyEvent;
import static store.jesframework.internal.Events.SampleEvent;

@Execution(CONCURRENT)
class SnapshotStrategyTest {

    @Test
    void alwaysSnapshotStrategyMustAlwaysDemandToSnapshot() {
        final AlwaysSnapshotStrategy strategy = new AlwaysSnapshotStrategy();
        assertTrue(strategy.isSnapshotNecessary(null, null));
        assertTrue(strategy.isSnapshotNecessary(new FancyAggregate(randomUUID()), null));
        assertTrue(strategy.isSnapshotNecessary(null, emptyList()));
        assertTrue(strategy.isSnapshotNecessary(new FancyAggregate(randomUUID()), emptyList()));
        assertTrue(strategy.isSnapshotNecessary(new FancyAggregate(randomUUID()),
                singleton(new SampleEvent("", randomUUID()))));
    }

    @Test
    void sizeBasedStrategyMustNotPermitNegativeSize() {
        assertThrows(IllegalArgumentException.class, () -> new SizeBasedSnapshotStrategy(0));
        assertThrows(IllegalArgumentException.class, () -> new SizeBasedSnapshotStrategy(-1));
        assertDoesNotThrow(() -> new SizeBasedSnapshotStrategy(1));
    }

    @Test
    void sizeBasedStrategyMustReactOnLoadedEventsSize() {
        final SizeBasedSnapshotStrategy strategy = new SizeBasedSnapshotStrategy(1);
        assertFalse(strategy.isSnapshotNecessary(new FancyAggregate(randomUUID()), singleton(new SampleEvent(""))));
        assertTrue(strategy.isSnapshotNecessary(new FancyAggregate(randomUUID()),
                asList(new SampleEvent(""), new FancyEvent("", randomUUID()))));
    }

    @Test
    @SuppressWarnings("ConstantConditions")
    void timeBasedStrategyMustNotPermitNullOrNegativeDuration() {
        assertDoesNotThrow(() -> new TimeBasedSnapshotStrategy(Duration.ofHours(1)));
        assertThrows(NullPointerException.class, () -> new TimeBasedSnapshotStrategy(null));
        assertThrows(IllegalArgumentException.class, () -> new TimeBasedSnapshotStrategy(Duration.ofNanos(-10)));
    }

    @Test
    @SneakyThrows
    void timeBasedStrategyMustReactOnLastSnapshotDate() {
        final Aggregate aggregate = new FancyAggregate(randomUUID());
        final Set<Event> loadedEvents = singleton(new SampleEvent(""));
        final TimeBasedSnapshotStrategy strategy = new TimeBasedSnapshotStrategy(Duration.ofMillis(50));

        assertFalse(strategy.isSnapshotNecessary(aggregate, loadedEvents));
        // wait for the specified duration
        TimeUnit.MILLISECONDS.sleep(50);
        assertTrue(strategy.isSnapshotNecessary(aggregate, loadedEvents));
    }

}