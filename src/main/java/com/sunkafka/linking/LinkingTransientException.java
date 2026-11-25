package com.sunkafka.linking;

/** The linking service could not be reached or failed (5xx, timeout, 408/429): worth retrying. */
public class LinkingTransientException extends RuntimeException {
    public LinkingTransientException(String message, Throwable cause) {
        super(message, cause);
    }
}
