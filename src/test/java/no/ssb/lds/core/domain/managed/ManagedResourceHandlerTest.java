package no.ssb.lds.core.domain.managed;

import com.fasterxml.jackson.databind.JsonNode;
import no.ssb.lds.api.persistence.DocumentKey;
import no.ssb.lds.api.persistence.Transaction;
import no.ssb.lds.api.persistence.json.JsonDocument;
import no.ssb.lds.api.persistence.json.JsonTools;
import no.ssb.lds.api.persistence.reactivex.RxJsonPersistence;
import no.ssb.lds.test.client.ResponseHelper;
import no.ssb.lds.test.client.TestClient;
import no.ssb.lds.test.server.TestServer;
import no.ssb.lds.test.server.TestServerListener;
import org.skyscreamer.jsonassert.JSONAssert;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import javax.inject.Inject;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Iterator;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Listeners(TestServerListener.class)
public class ManagedResourceHandlerTest {

    @Inject
    private TestClient client;

    @Inject
    private TestServer server;

    private void createTestResource(String entity, String id, String json) {
        ZonedDateTime timestamp = ZonedDateTime.now(ZoneId.of("Etc/UTC"));
        createTestResource(entity, id, timestamp, json);
    }

    private void createTestResource(String entity, String id, ZonedDateTime timestamp, String json) {
        JsonNode jsonObject = JsonTools.toJsonNode(json);
        RxJsonPersistence persistence = server.getApplication().getPersistence();
        try (Transaction tx = persistence.createTransaction(false)) {
            persistence.createOrOverwrite(tx, new JsonDocument(new DocumentKey("data", entity, id, timestamp), jsonObject), server.getApplication().getSpecification()).blockingAwait();
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
        client.delete("/data/provisionagreement/m2?sync=true").expect204NoContent();
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

    @Test
    public void thatTimelineIsCorrectForGivenResource() {
        createTestResource("provisionagreement", "5gTH4iagnrGHJ94gkf43JdcFB2",
                ZonedDateTime.of(2018, 2, 12, 12, 1, 0, 0, ZoneOffset.UTC),
                "{\"name\":\"first\"}");
        createTestResource("provisionagreement", "5gTH4iagnrGHJ94gkf43JdcFB2",
                ZonedDateTime.of(2018, 5, 3, 13, 2, 0, 0, ZoneOffset.UTC),
                "{\"name\":\"second\"}");
        createTestResource("provisionagreement", "5gTH4iagnrGHJ94gkf43JdcFB2",
                ZonedDateTime.of(2019, 9, 28, 14, 3, 0, 0, ZoneOffset.UTC),
                "{\"name\":\"third\"}");
        createTestResource("provisionagreement", "5gTH4iagnrGHJ94gkf43JdcFB2",
                ZonedDateTime.of(2020, 4, 30, 15, 4, 0, 0, ZoneOffset.UTC),
                "{\"name\":\"fourth\"}");

        ResponseHelper<String> response = client.get("/data/provisionagreement/5gTH4iagnrGHJ94gkf43JdcFB2?timeline").expect200Ok();

        JsonNode timelineResult = JsonTools.toJsonNode(response.body());
        assertTrue(timelineResult.isArray());
        assertEquals(timelineResult.size(), 4);
        Iterator<JsonNode> it = timelineResult.elements();
        JsonNode first = it.next();
        assertEquals(first.get("version").textValue(), "2018-02-12T12:01Z[Etc/UTC]");
        assertEquals(first.get("document").get("name").textValue(), "first");
        JsonNode second = it.next();
        assertEquals(second.get("version").textValue(), "2018-05-03T13:02Z[Etc/UTC]");
        assertEquals(second.get("document").get("name").textValue(), "second");
        JsonNode third = it.next();
        assertEquals(third.get("version").textValue(), "2019-09-28T14:03Z[Etc/UTC]");
        assertEquals(third.get("document").get("name").textValue(), "third");
        JsonNode fourth = it.next();
        assertEquals(fourth.get("version").textValue(), "2020-04-30T15:04Z[Etc/UTC]");
        assertEquals(fourth.get("document").get("name").textValue(), "fourth");
        assertFalse(it.hasNext());
    }

    @Test
    public void thatTimelineReturns404ForNonExistentResource() {
        client.get("/data/provisionagreement/non-existent-resource?timeline").expect404NotFound();
    }
}
