package no.ssb.lds.core.domain.resource;

import no.ssb.lds.core.linkeddata.LinkedData;
import no.ssb.lds.test.client.TestClient;
import no.ssb.lds.test.server.TestServer;
import no.ssb.lds.test.server.TestServerListener;
import org.json.JSONObject;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import javax.inject.Inject;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

import static org.testng.Assert.assertEquals;

@Listeners(TestServerListener.class)
public class ResourceContextIntegrationTest {

    @Inject
    private TestServer server;

    @Inject
    private TestClient client;

    private void createTestResource(String entity, String id, String body) {
        JSONObject jsonObject = new JSONObject(body);
        server.getPersistence().createOrOverwrite("data", entity, id, jsonObject,
                new LinkedData(server.getSpecification(), "data", entity, id, jsonObject).parse());
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
