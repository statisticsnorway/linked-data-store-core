package no.ssb.lds.core.domain.resource;

import com.fasterxml.jackson.databind.JsonNode;
import no.ssb.lds.api.persistence.DocumentKey;
import no.ssb.lds.api.persistence.Transaction;
import no.ssb.lds.api.persistence.json.JsonDocument;
import no.ssb.lds.api.persistence.json.JsonTools;
import no.ssb.lds.api.persistence.reactivex.RxJsonPersistence;
import no.ssb.lds.test.client.TestClient;
import no.ssb.lds.test.server.TestServer;
import no.ssb.lds.test.server.TestServerListener;
import org.testng.annotations.Ignore;
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
        JsonNode jsonObject = JsonTools.toJsonNode(json);
        RxJsonPersistence persistence = server.getApplication().getPersistence();
        try (Transaction tx = persistence.createTransaction(false)) {
            persistence.createOrOverwrite(tx, new JsonDocument(new DocumentKey("data", entity, id, timestamp), jsonObject), server.getApplication().getSpecification()).blockingAwait();
        }
    }

    static String urlEncode(String decoded) {
        return URLEncoder.encode(decoded, StandardCharsets.UTF_8);
    }

    @Ignore
    @Test
    public void thatFreakyResourceURLsAreDecodedProperly() {
        createTestResource("provisionagreement", "rc1", "{\"id\":\"rc1\",\"str/ange=Prop#Name\":\"some-value\"}");
        String response = client.get("/data/provisionagreement/rc1/" + urlEncode("str/ange=Prop#Name")).expect200Ok().body();
        assertEquals(response, "[\"some-value\"]");
    }
}
