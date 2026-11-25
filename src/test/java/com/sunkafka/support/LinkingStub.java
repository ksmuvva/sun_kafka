package com.sunkafka.support;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.matchingJsonPath;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.stubbing.Scenario;
import com.github.tomakehurst.wiremock.verification.LoggedRequest;
import java.util.List;

/**
 * A fake EU linking service shared by all tests (one server per JVM). Call {@link #reset()} before each test:
 * by default every POST /links succeeds with 201. Then script failures per order id.
 */
public final class LinkingStub {

    private static final WireMockServer SERVER = new WireMockServer(WireMockConfiguration.options().dynamicPort());

    static {
        SERVER.start();
        Runtime.getRuntime().addShutdownHook(new Thread(SERVER::stop));
    }

    private LinkingStub() {
    }

    public static String baseUrl() {
        return "http://localhost:" + SERVER.port();
    }

    /** Clears recorded requests and scripted behaviour; everything succeeds again. */
    public static void reset() {
        SERVER.resetAll();
        SERVER.stubFor(post(urlEqualTo("/links")).willReturn(aResponse().withStatus(201)));
    }

    /** Every call for this order answers with the given HTTP status. */
    public static void failWith(String orderId, int status) {
        SERVER.stubFor(post(urlEqualTo("/links")).withRequestBody(forOrder(orderId))
                .willReturn(aResponse().withStatus(status)));
    }

    /** Every call for this order succeeds, but only after {@code millis}. */
    public static void delay(String orderId, int millis) {
        SERVER.stubFor(post(urlEqualTo("/links")).withRequestBody(forOrder(orderId))
                .willReturn(aResponse().withStatus(201).withFixedDelay(millis)));
    }

    /** The first {@code failures} calls for this order fail with {@code status}; later calls succeed. */
    public static void failThenSucceed(String orderId, int status, int failures) {
        String scenario = "recovers-" + orderId;
        for (int i = 0; i < failures; i++) {
            String from = i == 0 ? Scenario.STARTED : "failed-" + i;
            SERVER.stubFor(post(urlEqualTo("/links")).withRequestBody(forOrder(orderId))
                    .inScenario(scenario).whenScenarioStateIs(from).willSetStateTo("failed-" + (i + 1))
                    .willReturn(aResponse().withStatus(status)));
        }
        SERVER.stubFor(post(urlEqualTo("/links")).withRequestBody(forOrder(orderId))
                .inScenario(scenario).whenScenarioStateIs("failed-" + failures)
                .willReturn(aResponse().withStatus(201)));
    }

    /** Calls the service received for this order. */
    public static List<LoggedRequest> requestsFor(String orderId) {
        return SERVER.findAll(postRequestedFor(urlEqualTo("/links")).withRequestBody(forOrder(orderId)));
    }

    private static com.github.tomakehurst.wiremock.matching.StringValuePattern forOrder(String orderId) {
        return matchingJsonPath("$.orderId", equalTo(orderId));
    }
}
