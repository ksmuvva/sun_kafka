package com.sunkafka;

import static com.sunkafka.support.OrderEvents.uniqueId;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.sunkafka.support.LinkingStub;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

/** The HTTP surface: POST /api/orders validates, publishes to Kafka, and the consumer links it. */
@AutoConfigureMockMvc
class EndpointTest extends KafkaE2ETestBase {

    @Autowired MockMvc mvc;

    @Test
    void validOrderIsAcceptedAndFlowsThroughKafkaToTheLinkingService() throws Exception {
        var id = uniqueId();

        mvc.perform(post("/api/orders").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"orderId\":\"" + id + "\",\"customer\":\"alice\",\"amount\":42.5}"))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.orderId").value(id));

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() ->
                assertThat(consumer.getReceived()).anyMatch(e -> e.getOrderId().equals(id)));
        assertThat(LinkingStub.requestsFor(id)).hasSize(1);
    }

    @Test
    void missingOrderIdIsGenerated() throws Exception {
        mvc.perform(post("/api/orders").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"customer\":\"alice\",\"amount\":10}"))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.orderId").isNotEmpty());
    }

    @Test
    void invalidOrderIsRejectedWithTheReasonAndNothingIsPublished() throws Exception {
        var id = uniqueId();

        mvc.perform(post("/api/orders").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"orderId\":\"" + id + "\",\"customer\":\"\",\"amount\":-3}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.title").value("Invalid order"))
                .andExpect(jsonPath("$.detail").value(org.hamcrest.Matchers.containsString("amount must be a positive number")))
                .andExpect(jsonPath("$.detail").value(org.hamcrest.Matchers.containsString("customer must not be blank")));

        assertThat(consumer.attemptsFor(id)).isZero();
        assertThat(LinkingStub.requestsFor(id)).isEmpty();
    }

    @Test
    void missingAmountIsRejected() throws Exception {
        mvc.perform(post("/api/orders").contentType(MediaType.APPLICATION_JSON).content("{\"customer\":\"alice\"}"))
                .andExpect(status().isBadRequest());
    }

    @Test
    void malformedJsonIsRejected() throws Exception {
        mvc.perform(post("/api/orders").contentType(MediaType.APPLICATION_JSON).content("{not json"))
                .andExpect(status().isBadRequest());
    }

    @Test
    void wrongContentTypeIsRejected() throws Exception {
        mvc.perform(post("/api/orders").contentType(MediaType.TEXT_PLAIN).content("hello"))
                .andExpect(status().isUnsupportedMediaType());
    }
}
