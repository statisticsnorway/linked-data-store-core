package no.ssb.lds.core.domain.reference;

import no.ssb.lds.api.persistence.Transaction;
import no.ssb.lds.api.persistence.buffered.BufferedPersistence;
import no.ssb.lds.api.persistence.buffered.DefaultBufferedPersistence;
import no.ssb.lds.api.persistence.buffered.Document;
import no.ssb.lds.core.buffered.JsonToDocument;
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

@Listeners(TestServerListener.class)
public class ReferenceResourceHandlerTest {

    @Inject
    private TestClient client;

    @Inject
    private TestServer server;

    private void createTestResource(String entity, String id, String json) {
        ZonedDateTime timestamp = ZonedDateTime.now(ZoneId.of("Etc/UTC"));
        JSONObject jsonObject = new JSONObject(json);
        Document document = new JsonToDocument("data", entity, id, timestamp, jsonObject, 8 * 1024).toDocument();
        BufferedPersistence persistence = new DefaultBufferedPersistence(server.getPersistence(), 8 * 1024);
        try (Transaction tx = persistence.createTransaction(false)) {
            persistence.createOrOverwrite(tx, document);
        }
    }

    @Test
    public void thatGETMissingReferenceReturnsWith404() {
        createTestResource("provisionagreement", "r1", "{\"id\":\"r1\",\"name\":\"pa-test-name\",\"contacts\":[\"/contact/c1\",\"/contact/c2\"]}");
        client.get("/data/provisionagreement/r1/contacts/contact/c3").expect404NotFound();
    }

    @Test
    public void thatGETReferenceReturnsWith200() {
        createTestResource("provisionagreement", "r2", "{\"id\":\"r2\",\"name\":\"pa-test-name\",\"contacts\":[\"/contact/c1\",\"/contact/c2\"]}");
        client.get("/data/provisionagreement/r2/contacts/contact/c1").expect200Ok();
    }

    @Test
    public void thatPUTAddArrayReferenceReturns200() {
        createTestResource("provisionagreement", "r3", "{\"id\":\"r3\",\"name\":\"pa-test-name\"}");
        client.get("/data/provisionagreement/r3/contacts/contact/c3").expect404NotFound();
        client.put("/data/provisionagreement/r3/contacts/contact/c3?sync=true").expect200Ok();
        client.get("/data/provisionagreement/r3/contacts/contact/c3").expect200Ok();
    }

    @Test
    public void thatPUTCreateArrayReferenceReturns200() {
        createTestResource("provisionagreement", "r4", "{\"id\":\"r4\",\"name\":\"pa-test-name\"}");
        client.get("/data/provisionagreement/r4/contacts/contact/c3").expect404NotFound();
        client.put("/data/provisionagreement/r4/contacts/contact/c3?sync=true").expect200Ok();
        client.get("/data/provisionagreement/r4/contacts/contact/c3").expect200Ok();
    }

    @Test
    public void thatPUTCreateSingleReferenceReturns200() {
        createTestResource("provisionagreement", "r5", "{\"id\":\"r5\",\"name\":\"pa-test-name\",\"contacts\":[\"/contact/c1\",\"/contact/c2\"]}");
        client.get("/data/provisionagreement/r5/friend/contact/f1").expect404NotFound();
        client.put("/data/provisionagreement/r5/friend/contact/f1?sync=true").expect200Ok();
        client.get("/data/provisionagreement/r5/friend/contact/f1").expect200Ok();
    }

    @Test
    public void thatPUTOverwriteSingleReferenceReturns200() {
        createTestResource("provisionagreement", "r6", "{\"id\":\"r6\",\"name\":\"pa-test-name\",\"contacts\":[\"/contact/c1\",\"/contact/c2\"],\"friend\":\"/contact/not-a-friend\"}");
        client.get("/data/provisionagreement/r6/friend/contact/f1").expect404NotFound();
        client.put("/data/provisionagreement/r6/friend/contact/f1?sync=true").expect200Ok();
        client.get("/data/provisionagreement/r6/friend/contact/f1").expect200Ok();
    }

    @Test
    public void thatDELETEArrayReferenceReturns200() {
        createTestResource("provisionagreement", "r7", "{\"id\":\"r7\",\"contacts\":[\"/contact/c1\",\"/contact/c2\"]}");
        client.get("/data/provisionagreement/r7/contacts/contact/c1").expect200Ok();
        client.delete("/data/provisionagreement/r7/contacts/contact/c1?sync=true").expect200Ok();
        client.get("/data/provisionagreement/r7/contacts/contact/c1").expect404NotFound();
    }

    @Test
    public void thatDELETESingleReferenceReturns200() {
        createTestResource("provisionagreement", "r8", "{\"id\":\"r8\",\"friend\":\"/contact/f1\"}");
        client.get("/data/provisionagreement/r8/friend/contact/f1").expect200Ok();
        client.delete("/data/provisionagreement/r8/friend/contact/f1?sync=true").expect200Ok();
        client.get("/data/provisionagreement/r8/friend/contact/f1").expect404NotFound();
    }

    @Test
    public void thatDELETENonResourceReturns200() {
        createTestResource("provisionagreement", "r9", "{\"id\":\"r9\"}");
        client.get("/data/provisionagreement/r9/friend/contact/f1").expect404NotFound();
        client.delete("/data/provisionagreement/r9/friend/contact/f1").expect200Ok();
    }

    @Test
    public void thatDELETEResourceIsIdempotent() {
        createTestResource("provisionagreement", "r10", "{\"id\":\"r10\",\"friend\":\"/contact/f1\"}");
        client.get("/data/provisionagreement/r10/friend/contact/f1").expect200Ok();
        client.delete("/data/provisionagreement/r10/friend/contact/f1?sync=true").expect200Ok();
        client.get("/data/provisionagreement/r10/friend/contact/f1").expect404NotFound();
        client.delete("/data/provisionagreement/r10/friend/contact/f1").expect200Ok();
        client.get("/data/provisionagreement/r10/friend/contact/f1").expect404NotFound();
    }

    @Test
    public void thatDELETEDoesNotDeleteTopLevelResource() {
        createTestResource("provisionagreement", "r11", "{\"id\":\"r11\",\"friend\":\"/contact/f1\"}");
        client.get("/data/provisionagreement/r11").expect200Ok();
        client.get("/data/provisionagreement/r11/friend/contact/f1").expect200Ok();
        client.delete("/data/provisionagreement/r11/friend/contact/f1?sync=true").expect200Ok();
        client.get("/data/provisionagreement/r11/friend/contact/f1").expect404NotFound();
        client.get("/data/provisionagreement/r11").expect200Ok();
    }

    @Test
    public void thatDELETERefInArrayDoesNotDeleteSiblingResource() {
        createTestResource("provisionagreement", "r12", "{\"id\":\"r12\",\"name\":\"pa-test-name\",\"contacts\":[\"/contact/c1\"]}");
        client.get("/data/provisionagreement/r12/contacts/contact/c1").expect200Ok();
        client.get("/data/provisionagreement/r12/contacts/contact/c2").expect404NotFound();
        client.put("/data/provisionagreement/r12/contacts/contact/c2?sync=true").expect200Ok();
        client.get("/data/provisionagreement/r12/contacts/contact/c2").expect200Ok();
        client.delete("/data/provisionagreement/r12/contacts/contact/c2?sync=true").expect200Ok();
        client.get("/data/provisionagreement/r12/contacts/contact/c2").expect404NotFound();
        client.get("/data/provisionagreement/r12/contacts/contact/c1").expect200Ok();
    }

    @Test
    public void thatDELETELongResourcePathWorks() {
        createTestResource("provisionagreement", "r13", "{\"id\":\"r13\",\"name\":\"pa-test-name\",\"support\":{\"technicalSupport\":[\"/contact/s1\",\"/contact/s2\"],\"businessSupport\":[\"/contact/b1\"]}}");
        client.delete("/data/provisionagreement/r13/support/technicalSupport/contact/s1?sync=true").expect200Ok();
        String response = client.get("/data/provisionagreement/r13/support").expect200Ok().body();
        JSONAssert.assertEquals("{\"technicalSupport\":[\"/contact/s2\"],\"businessSupport\":[\"/contact/b1\"]}", response, true);
    }

    @Test
    public void thatPUTLongResourcePathWorks() {
        createTestResource("provisionagreement", "r14", "{\"id\":\"r14\",\"name\":\"pa-test-name\",\"support\":{\"technicalSupport\":[\"/contact/s1\",\"/contact/s2\"],\"businessSupport\":[\"/contact/b1\"]}}");
        client.put("/data/provisionagreement/r14/support/technicalSupport/contact/s3?sync=true", "[]").expect200Ok();
        String response = client.get("/data/provisionagreement/r14/support").expect200Ok().body();
        JSONAssert.assertEquals("{\"technicalSupport\":[\"/contact/s1\",\"/contact/s2\",\"/contact/s3\"],\"businessSupport\":[\"/contact/b1\"]}", response, true);
    }

    @Test
    public void thatGETLongResourcePathWorks() {
        client.delete("/data/provisionagreement/r15?sync=true").expectAnyOf(200, 204);
        client.get("/data/provisionagreement/r15/support/technicalSupport/contact/s2").expect404NotFound();
        createTestResource("provisionagreement", "r15", "{\"id\":\"r15\",\"name\":\"pa-test-name\",\"support\":{\"technicalSupport\":[\"/contact/s1\",\"/contact/s2\"],\"businessSupport\":[\"/contact/b1\"]}}");
        client.get("/data/provisionagreement/r15/support/technicalSupport/contact/s2").expect200Ok();
        client.get("/data/provisionagreement/r15/support/technicalSupport/contact/s3").expect404NotFound();
    }

    @Test
    public void thatGETWrongTypedReferenceReturns400() {
        client.get("/data/provisionagreement/a/contacts/evil/c1").expect400BadRequest();
    }
}
