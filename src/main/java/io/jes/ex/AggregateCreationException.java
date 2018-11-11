package io.jes.ex;

import io.jes.aggregate.Aggregate;

public class AggregateCreationException extends RuntimeException {

    private static final String ERROR = "Aggregate of type: %s cannot be created. Reason: %s";

    public <T extends Aggregate> AggregateCreationException(Class<T> type, Throwable cause) {
        super(String.format(ERROR, type, cause.getMessage()), cause);
    }
}
