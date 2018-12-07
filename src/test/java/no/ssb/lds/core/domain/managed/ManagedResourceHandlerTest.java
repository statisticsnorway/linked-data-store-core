package no.ssb.lds.core.domain.managed;

import no.ssb.lds.api.persistence.Transaction;
import no.ssb.lds.api.persistence.buffered.BufferedPersistence;
import no.ssb.lds.api.persistence.buffered.DefaultBufferedPersistence;
import no.ssb.lds.api.persistence.buffered.FlattenedDocument;
import no.ssb.lds.core.buffered.JsonToDocument;
import no.ssb.lds.test.client.ResponseHelper;
import no.ssb.lds.test.client.TestClient;
import no.ssb.lds.test.server.TestServer;
import no.ssb.lds.test.server.TestServerListener;
import org.json.JSONObject;
import org.skyscreamer.jsonassert.JSONAssert;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import javax.inject.Inject;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Listeners(TestServerListener.class)
public class ManagedResourceHandlerTest {

    @Inject
    private TestClient client;

    @Inject
    private TestServer server;

    private void createTestResource(String entity, String id, String json) {
        ZonedDateTime timestamp = ZonedDateTime.now(ZoneId.of("Etc/UTC"));
        JSONObject jsonObject = new JSONObject(json);
        FlattenedDocument document = new JsonToDocument("data", entity, id, timestamp, jsonObject, 8 * 1024).toDocument();
        BufferedPersistence persistence = new DefaultBufferedPersistence(server.getPersistence(), 8 * 1024);
        try (Transaction tx = persistence.createTransaction(false)) {
            persistence.createOrOverwrite(tx, document);
        }
    }

    @Test
    public void thatPUTIntegrationWorks() {
        String body = "{\"name\":\"pa-test-name\",\"contacts\":[\"/contact/c1\",\"/contact/c2\"]}";
        client.put("/data/provisionagreement/m1?sync=true", body).expectAnyOf(200, 201);
        String actual = client.get("/data/provisionagreement/m1").expect200Ok().body();
        JSONAssert.assertEquals(body, actual, false);
    }

    @Test
    public void thatDELETEDoesRemoveResource() {
        createTestResource("provisionagreement", "m2", "{\"name\":\"pa-test-name\",\"contacts\":[\"/contact/c1\",\"/contact/c2\"]}");
        client.get("/data/provisionagreement/m2").expect200Ok();
        client.delete("/data/provisionagreement/m2?sync=true").expect200Ok();
        client.get("/data/provisionagreement/m2").expect404NotFound();
    }

    @Test
    public void thatNonExistentResourcesReturnsWith404NotFoundAndDoesNotLeakContext() {
        String response1 = client.get("/data/provisionagreement/m3").expect404NotFound().body();
        String response2 = client.get("/data/contact/m3c1").expect404NotFound().body();
        // check that no context is leaked, i.e. response is same for different non-existent resources
        assertEquals(response1, response2);
    }

    @Test
    public void thatJsonSchemaIsFoundForNamespace() {
        ResponseHelper<String> response = client.get("/data/contact?schema");
        assertTrue(response.body().contains("\"$ref\":\"#/definitions/contact\""));
    }
}
