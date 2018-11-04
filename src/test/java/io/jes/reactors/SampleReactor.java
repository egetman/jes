package io.jes.reactors;

import java.util.concurrent.CountDownLatch;
import javax.annotation.Nonnull;

import io.jes.JEventStore;
import io.jes.common.FancyEvent;
import io.jes.common.ProcessingTerminated;
import io.jes.common.SampleEvent;
import io.jes.offset.Offset;

@SuppressWarnings("unused")
class SampleReactor extends Reactor {

    @Nonnull
    private final CountDownLatch endLatch;
    @Nonnull
    private final CountDownLatch startLatch;
    @Nonnull
    private final CountDownLatch fancyLatch;
    @Nonnull
    private final CountDownLatch sampleLatch;

    private volatile boolean terminated;

    SampleReactor(@Nonnull JEventStore store, @Nonnull Offset offset,
                  @Nonnull CountDownLatch endLatch,
                  @Nonnull CountDownLatch startLatch,
                  @Nonnull CountDownLatch fancyLatch,
                  @Nonnull CountDownLatch sampleLatch) {

        super(store, offset);
        this.startLatch = startLatch;
        this.endLatch = endLatch;
        this.fancyLatch = fancyLatch;
        this.sampleLatch = sampleLatch;
    }


    @Handler
    private void handle(ProcessingTerminated event) {
        this.terminated = true;
    }

    @Handler
    private void handle(SampleEvent event) {
        sampleLatch.countDown();
    }

    @Handler
    private void handle(FancyEvent event) {
        fancyLatch.countDown();
    }

    @Override
    void tailStore() {
        super.tailStore();
        startLatch.countDown();
        if (terminated) {
            endLatch.countDown();
        }
    }
}
