package com.sunkafka.linking;

/** The linking service rejected the request (4xx other than 408/429): retrying cannot help. */
public class LinkingPermanentException extends RuntimeException {
    public LinkingPermanentException(String message, Throwable cause) {
        super(message, cause);
    }
}
