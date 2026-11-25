package com.sunkafka.web;

import com.sunkafka.consumer.InvalidOrderException;
import com.sunkafka.model.OrderEvent;
import com.sunkafka.producer.OrderProducer;
import com.sunkafka.validation.OrderValidator;
import java.util.Map;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.bind.annotation.RestController;

/** Lets a client submit an order over HTTP; it is validated, then published to Kafka. */
@RestController
@RequestMapping("/api/orders")
public class OrderController {

    /** Request body. {@code orderId} and {@code currency} are optional. */
    public record OrderRequest(String orderId, String customer, Double amount, String currency) {
    }

    private final OrderValidator validator;
    private final OrderProducer producer;

    public OrderController(OrderValidator validator, OrderProducer producer) {
        this.validator = validator;
        this.producer = producer;
    }

    @PostMapping
    public ResponseEntity<Map<String, String>> submit(@RequestBody OrderRequest request) {
        var event = OrderEvent.newBuilder()
                .setOrderId(request.orderId() == null ? UUID.randomUUID().toString() : request.orderId())
                .setCustomer(request.customer() == null ? "" : request.customer())
                .setAmount(request.amount() == null ? Double.NaN : request.amount())
                .setCurrency(request.currency() == null ? "USD" : request.currency())
                .build();
        validator.validate(event);
        producer.send(event).join();
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(Map.of("orderId", event.getOrderId()));
    }

    @RestControllerAdvice
    static class Errors {
        @ExceptionHandler(InvalidOrderException.class)
        ResponseEntity<ProblemDetail> invalid(InvalidOrderException e) {
            var problem = ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, e.getMessage());
            problem.setTitle("Invalid order");
            return ResponseEntity.badRequest().body(problem);
        }
    }
}
