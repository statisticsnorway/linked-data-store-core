package no.ssb.lds.core.controller;

import no.ssb.lds.test.client.ResponseHelper;
import no.ssb.lds.test.client.TestClient;
import no.ssb.lds.test.server.TestServerListener;
import org.json.JSONArray;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import javax.inject.Inject;

import static org.testng.Assert.assertTrue;

@Listeners(TestServerListener.class)
public class NamespaceSchemaTest {

    @Inject
    private TestClient client;

    @Test
    public void thatNamespaceSchemaRespondsWithSetOfManagedDomains() {
        ResponseHelper<String> response = client.get("/data?schema");
        assertTrue( new JSONArray(response.expect200Ok().body()).length() > 1);
    }

    @Test
    public void thatNamespaceSchemaRespondsWithEmbeddedManagedDomains() {
        ResponseHelper<String> response = client.get("/data?schema=embed");
        assertTrue( new JSONArray(response.expect200Ok().body()).length() > 1);
    }
}
