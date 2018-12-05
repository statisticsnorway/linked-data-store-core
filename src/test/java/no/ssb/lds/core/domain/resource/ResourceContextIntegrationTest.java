package no.ssb.lds.core.domain.resource;

import no.ssb.lds.api.persistence.Transaction;
import no.ssb.lds.api.persistence.buffered.BufferedPersistence;
import no.ssb.lds.api.persistence.buffered.DefaultBufferedPersistence;
import no.ssb.lds.api.persistence.buffered.Document;
import no.ssb.lds.core.buffered.JsonToDocument;
import no.ssb.lds.test.client.TestClient;
import no.ssb.lds.test.server.TestServer;
import no.ssb.lds.test.server.TestServerListener;
import org.json.JSONObject;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import javax.inject.Inject;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import static org.testng.Assert.assertEquals;

@Listeners(TestServerListener.class)
public class ResourceContextIntegrationTest {

    @Inject
    private TestServer server;

    @Inject
    private TestClient client;

    private void createTestResource(String entity, String id, String json) {
        ZonedDateTime timestamp = ZonedDateTime.now(ZoneId.of("Etc/UTC"));
        JSONObject jsonObject = new JSONObject(json);
        Document document = new JsonToDocument("data", entity, id, timestamp, jsonObject, 8 * 1024).toDocument();
        BufferedPersistence persistence = new DefaultBufferedPersistence(server.getPersistence(), 8 * 1024);
        try (Transaction tx = persistence.createTransaction(false)) {
            persistence.createOrOverwrite(tx, document);
        }
    }

    static String urlEncode(String decoded) {
        return URLEncoder.encode(decoded, StandardCharsets.UTF_8);
    }

    @Test
    public void thatFreakyResourceURLsAreDecodedProperly() {
        createTestResource("provisionagreement", "rc1", "{\"id\":\"rc1\",\"str/ange=Prop#Name\":\"some-value\"}");
        String response = client.get("/data/provisionagreement/rc1/" + urlEncode("str/ange=Prop#Name")).expect200Ok().body();
        assertEquals(response, "[\"some-value\"]");
    }
}
