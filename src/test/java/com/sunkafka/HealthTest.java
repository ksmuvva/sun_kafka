package com.sunkafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.sunkafka.health.KafkaHealthIndicator;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.health.Status;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.test.web.servlet.MockMvc;

/** Health and readiness: UP when Kafka and the database are reachable, DOWN when they are not. */
@AutoConfigureMockMvc
class HealthTest extends KafkaE2ETestBase {

    @Autowired MockMvc mvc;

    @Test
    void healthIsUpAndReportsKafkaAndDatabase() throws Exception {
        mvc.perform(get("/actuator/health"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("UP"))
                .andExpect(jsonPath("$.components.kafka.status").value("UP"))
                .andExpect(jsonPath("$.components.db.status").value("UP"));
    }

    @Test
    void readinessIsUp() throws Exception {
        mvc.perform(get("/actuator/health/readiness"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("UP"));
    }

    @Test
    void kafkaIndicatorIsDownWhenTheBrokerCannotBeReached() {
        var indicator = new KafkaHealthIndicator("localhost:1", Duration.ofMillis(500));

        assertThat(indicator.health().getStatus()).isEqualTo(Status.DOWN);
    }
}
