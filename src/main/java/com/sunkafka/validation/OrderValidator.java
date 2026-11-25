package com.sunkafka.validation;

import com.sunkafka.consumer.InvalidOrderException;
import com.sunkafka.model.OrderEvent;
import java.util.ArrayList;
import org.springframework.stereotype.Component;

/** The single place that decides whether an order is acceptable; used by the consumer and the REST endpoint. */
@Component
public class OrderValidator {

    /** @throws InvalidOrderException listing every rule that failed */
    public void validate(OrderEvent event) {
        var problems = new ArrayList<String>();
        if (isBlank(event.getOrderId())) {
            problems.add("orderId must not be blank");
        }
        if (isBlank(event.getCustomer())) {
            problems.add("customer must not be blank");
        }
        double amount = event.getAmount();
        if (Double.isNaN(amount) || Double.isInfinite(amount) || amount <= 0) {
            problems.add("amount must be a positive number");
        }
        String currency = event.getCurrency();
        if (currency == null || !currency.matches("[A-Z]{3}")) {
            problems.add("currency must be a 3-letter upper-case code");
        }
        if (!problems.isEmpty()) {
            throw new InvalidOrderException("Invalid order " + event.getOrderId() + ": " + String.join("; ", problems));
        }
    }

    private static boolean isBlank(CharSequence value) {
        return value == null || value.toString().isBlank();
    }
}
