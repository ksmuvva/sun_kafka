package com.sunkafka.support;

import com.sunkafka.model.OrderEvent;
import java.util.UUID;

/** Test-data builders. */
public final class OrderEvents {

    private OrderEvents() {
    }

    public static String uniqueId() {
        return "o-" + UUID.randomUUID();
    }

    public static OrderEvent valid(String orderId) {
        return new OrderEvent(orderId, "alice", 42.5);
    }

    public static OrderEvent withAmount(String orderId, double amount) {
        return new OrderEvent(orderId, "alice", amount);
    }
}
