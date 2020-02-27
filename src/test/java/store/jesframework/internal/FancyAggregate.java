package store.jesframework.internal;

import java.util.UUID;

import lombok.Data;
import lombok.EqualsAndHashCode;
import store.jesframework.Aggregate;
import store.jesframework.internal.Events.FancyEvent;
import store.jesframework.internal.Events.ProcessingTerminated;

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
        registerApplier(FancyEvent.class, this::handle);
        registerApplier(ProcessingTerminated.class, this::handle);
    }

    private void handle(FancyEvent fancyEvent) {
        fancyName = fancyEvent.getName();
    }

    @SuppressWarnings("unused")
    private void handle(ProcessingTerminated processingTerminated) {
        cancelled = true;
    }
}
