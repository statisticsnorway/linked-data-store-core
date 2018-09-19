package no.ssb.lds.core.controller;

import no.ssb.lds.test.client.TestClient;
import no.ssb.lds.test.server.TestServerListener;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import javax.inject.Inject;

import static org.testng.Assert.assertEquals;

@Listeners(TestServerListener.class)
public class DataControllerTest {

    @Inject
    private TestClient client;

    @Test
    public void thatGETWithIllegalRequestPathFailsWith400() {
        String response = client.get("/data/bull/foo/bar").expect400BadRequest().body();
        assertEquals(response, "Not a managed resource name: \"bull\"");
    }
}
