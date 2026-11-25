package com.sunkafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sunkafka.consumer.InvalidOrderException;
import com.sunkafka.model.OrderEvent;
import com.sunkafka.validation.OrderValidator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/** Fast unit tests (no Kafka, no Spring) for the single set of business rules. */
class ValidationTest {

    private final OrderValidator validator = new OrderValidator();

    private static OrderEvent order(String id, String customer, double amount, String currency) {
        return OrderEvent.newBuilder().setOrderId(id).setCustomer(customer).setAmount(amount).setCurrency(currency).build();
    }

    @Test
    void validOrderPasses() {
        assertThatCode(() -> validator.validate(order("o-1", "alice", 10.5, "GBP"))).doesNotThrowAnyException();
    }

    @ParameterizedTest(name = "{0}")
    @CsvSource(delimiter = '|', value = {
            "blank order id        | ' '   | alice | 10   | USD | orderId must not be blank",
            "blank customer        | o-1   | ' '   | 10   | USD | customer must not be blank",
            "zero amount           | o-1   | alice | 0    | USD | amount must be a positive number",
            "negative amount       | o-1   | alice | -5   | USD | amount must be a positive number",
            "not-a-number amount   | o-1   | alice | NaN  | USD | amount must be a positive number",
            "lower-case currency   | o-1   | alice | 10   | usd | currency must be a 3-letter upper-case code",
            "too-long currency     | o-1   | alice | 10   | USDX| currency must be a 3-letter upper-case code"})
    void invalidOrderIsRejectedWithTheReason(String name, String id, String customer, double amount, String currency,
                                             String expectedProblem) {
        assertThatThrownBy(() -> validator.validate(order(id.trim(), customer.trim(), amount, currency.trim())))
                .isInstanceOf(InvalidOrderException.class)
                .hasMessageContaining(expectedProblem);
    }

    @Test
    void allProblemsAreReportedTogether() {
        assertThatThrownBy(() -> validator.validate(order("o-1", " ", -1, "x")))
                .isInstanceOf(InvalidOrderException.class)
                .satisfies(e -> assertThat(e.getMessage())
                        .contains("customer").contains("amount").contains("currency"));
    }
}
