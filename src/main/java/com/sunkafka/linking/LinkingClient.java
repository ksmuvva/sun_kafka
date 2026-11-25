package com.sunkafka.linking;

import com.sunkafka.model.OrderEvent;
import java.time.Duration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

/**
 * Calls the EU linking service. The real API is not known yet (see docs/REQUIREMENTS.md, Q1), so the path, body and
 * the {@code Idempotency-Key} header here are assumptions; only this class needs to change when the contract arrives.
 */
@Component
public class LinkingClient {

    /** Request body sent to the linking service (assumed shape). */
    public record LinkRequest(String orderId, String customer, double amount, String currency) {
    }

    private final RestClient client;

    public LinkingClient(@Value("${linking.base-url}") String baseUrl,
                         @Value("${linking.connect-timeout:1s}") Duration connectTimeout,
                         @Value("${linking.read-timeout:2s}") Duration readTimeout) {
        var factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(connectTimeout);
        factory.setReadTimeout(readTimeout);
        this.client = RestClient.builder().baseUrl(baseUrl).requestFactory(factory).build();
    }

    /**
     * @param idempotencyKey identifies this Kafka record, so the linking service can ignore a repeated call
     * @throws LinkingTransientException for timeouts, connection errors, 5xx, 408 and 429 (retry)
     * @throws LinkingPermanentException for other 4xx (do not retry)
     */
    public void link(OrderEvent event, String idempotencyKey) {
        try {
            client.post()
                    .uri("/links")
                    .header("Idempotency-Key", idempotencyKey)
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(new LinkRequest(event.getOrderId(), event.getCustomer(), event.getAmount(), event.getCurrency()))
                    .retrieve()
                    .toBodilessEntity();
        } catch (HttpClientErrorException e) {
            int status = e.getStatusCode().value();
            if (status == 408 || status == 429) {
                throw new LinkingTransientException("Linking service asked us to retry (HTTP " + status + ")", e);
            }
            throw new LinkingPermanentException("Linking service rejected the request (HTTP " + status + ")", e);
        } catch (RestClientException e) {
            // 5xx, timeouts and connection failures
            throw new LinkingTransientException("Linking service call failed: " + e.getMessage(), e);
        }
    }
}
