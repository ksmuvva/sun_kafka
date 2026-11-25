package com.sunkafka.bdd;

import com.sunkafka.support.LinkingStub;
import io.cucumber.java.Before;

/** Every scenario starts with a healthy fake linking service. */
public class StubHooks {

    @Before(order = 0)
    public void resetLinkingStub() {
        LinkingStub.reset();
    }
}
