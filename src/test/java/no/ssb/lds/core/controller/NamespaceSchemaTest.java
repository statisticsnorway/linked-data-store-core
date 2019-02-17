package no.ssb.lds.core.controller;

import no.ssb.lds.test.client.ResponseHelper;
import no.ssb.lds.test.client.TestClient;
import no.ssb.lds.test.server.TestServerListener;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import javax.inject.Inject;
import java.io.IOException;

import static no.ssb.lds.api.persistence.json.JsonDocument.mapper;
import static org.testng.Assert.assertTrue;

@Listeners(TestServerListener.class)
public class NamespaceSchemaTest {

    @Inject
    private TestClient client;

    @Test
    public void thatNamespaceSchemaRespondsWithSetOfManagedDomains() throws IOException {
        ResponseHelper<String> response = client.get("/data?schema");
        assertTrue(mapper.readTree(response.expect200Ok().body()).size() > 1);
    }

    @Test
    public void thatNamespaceSchemaRespondsWithEmbeddedManagedDomains() throws IOException {
        ResponseHelper<String> response = client.get("/data?schema=embed");
        assertTrue(mapper.readTree(response.expect200Ok().body()).size() > 1);
    }
}
