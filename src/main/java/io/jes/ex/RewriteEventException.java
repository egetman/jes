package io.jes.ex;

import io.jes.Event;

public class RewriteEventException extends RuntimeException {

    public RewriteEventException(Event event) {
        super("Rewriting event more than ones not allowed. Failed event: " + event);
    }
}
