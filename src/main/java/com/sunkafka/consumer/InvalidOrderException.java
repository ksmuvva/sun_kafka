package com.sunkafka.consumer;

/** Business-validation failure: retried, then routed to the dead-letter topic. */
public class InvalidOrderException extends RuntimeException {
    public InvalidOrderException(String message) {
        super(message);
    }
}
