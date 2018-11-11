package io.jes.internal;

import io.jes.aggregate.SimpleAggregate;
import io.jes.internal.Events.FancyEvent;
import io.jes.internal.Events.ProcessingTerminated;
import io.jes.internal.Events.SampleEvent;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = false)
public class FancyAggregate extends SimpleAggregate {

    private String fancyName;
    private int version;
    private boolean cancelled;


    public FancyAggregate() {
        registerApplier(SampleEvent.class, this::handle);
        registerApplier(FancyEvent.class, this::handle);
        registerApplier(ProcessingTerminated.class, this::handle);
    }

    // assume it's first (initial) event in use case
    private void handle(SampleEvent event) {
        version++;
        uuid = event.uuid();
    }

    private void handle(FancyEvent fancyEvent) {
        fancyName = fancyEvent.getName();
        version++;
    }

    private void handle(ProcessingTerminated processingTerminated) {
        cancelled = true;
        version++;
    }
}
