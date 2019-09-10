package store.jesframework.internal;

import java.util.UUID;

import store.jesframework.Aggregate;
import store.jesframework.internal.Events.FancyEvent;
import store.jesframework.internal.Events.ProcessingTerminated;
import store.jesframework.internal.Events.SampleEvent;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = false)
public class FancyAggregate extends Aggregate {

    private String fancyName;
    private boolean cancelled;

    public FancyAggregate(UUID uuid) {
        this();
        this.uuid = uuid;
    }

    protected FancyAggregate() {
        registerApplier(SampleEvent.class, this::handle);
        registerApplier(FancyEvent.class, this::handle);
        registerApplier(ProcessingTerminated.class, this::handle);
    }

    // assume it's first (initial) event in use case
    private void handle(SampleEvent event) {
        uuid = event.uuid();
    }

    private void handle(FancyEvent fancyEvent) {
        fancyName = fancyEvent.getName();
    }

    @SuppressWarnings("unused")
    private void handle(ProcessingTerminated processingTerminated) {
        cancelled = true;
    }
}
