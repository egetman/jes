package store.jesframework.reactors;

import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import javax.annotation.Nonnull;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import lombok.SneakyThrows;
import store.jesframework.lock.InMemoryReentrantLock;

import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TriggersTest {

    private static final long DEFAULT_DELAY_MS = 100;

    private static final Collection<Trigger> TRIGGERS = asList(
            new PollingTrigger(),
            new BlockingPollingTrigger(new InMemoryReentrantLock())
    );

    private static Collection<Trigger> triggers() {
        return TRIGGERS;
    }

    @ParameterizedTest
    @MethodSource("triggers")
    @SuppressWarnings("ConstantConditions")
    void pollingTriggerShouldThrowNullPointerExceptionOnNullAction(@Nonnull Trigger trigger) {
        assertThrows(NullPointerException.class, () -> trigger.onChange("", null));
    }

    @ParameterizedTest
    @MethodSource("triggers")
    @SuppressWarnings("ConstantConditions")
    void pollingTriggerShouldThrowNullPointerExceptionOnNullKey(@Nonnull Trigger trigger) {
        assertThrows(NullPointerException.class, () -> trigger.onChange(null, () -> {}));
    }

    @SneakyThrows
    @ParameterizedTest
    @MethodSource("triggers")
    void pollingTriggerShouldTriggerActionAfterDefaultDelay(@Nonnull Trigger trigger) {
        final CountDownLatch latch = new CountDownLatch(1);
        trigger.onChange("", latch::countDown);
        long delay = trigger instanceof PollingTrigger ? ((PollingTrigger) trigger).getDelay() : DEFAULT_DELAY_MS;
        assertTrue(latch.await(delay * 2, MILLISECONDS));
    }

    @AfterAll
    static void cleanUp() {
        for (Trigger trigger : TRIGGERS) {
            try {
                trigger.close();
            } catch (Exception ignored) {}
        }
    }

}