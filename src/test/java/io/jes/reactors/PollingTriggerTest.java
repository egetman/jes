package io.jes.reactors;

import java.util.concurrent.CountDownLatch;

import org.junit.jupiter.api.Test;

import lombok.SneakyThrows;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PollingTriggerTest {

    @Test
    @SuppressWarnings("ConstantConditions")
    void pollingTriggerShouldThrowNullPointerExceptionOnNullAction() {
        assertThrows(NullPointerException.class, () -> {
            try (PollingTrigger pollingTrigger = new PollingTrigger()) {
                pollingTrigger.onChange(null);
            }
        });
    }

    @Test
    @SneakyThrows
    void pollingTriggerShouldTriggerActionAfterDefaultDelay() {
        try (PollingTrigger trigger = new PollingTrigger()) {
            final CountDownLatch latch = new CountDownLatch(1);
            trigger.onChange(latch::countDown);
            assertTrue(latch.await(trigger.getDelay() * 2, MILLISECONDS));
        }
    }


}