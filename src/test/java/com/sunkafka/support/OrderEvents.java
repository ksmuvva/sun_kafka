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
        return withAmount(orderId, 42.5);
    }

    public static OrderEvent withAmount(String orderId, double amount) {
        return OrderEvent.newBuilder()
                .setOrderId(orderId).setCustomer("alice").setAmount(amount).build();
    }
}
