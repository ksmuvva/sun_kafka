package com.sunkafka.model;

/** Simple payload exchanged over Kafka. */
public record OrderEvent(String orderId, String customer, double amount) {
}
