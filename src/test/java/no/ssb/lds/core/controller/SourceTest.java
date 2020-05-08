package no.ssb.lds.core.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import no.ssb.lds.api.persistence.json.JsonTools;
import no.ssb.lds.test.ConfigurationOverride;
import no.ssb.lds.test.client.TestClient;
import no.ssb.lds.test.server.TestServer;
import no.ssb.lds.test.server.TestServerListener;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import javax.inject.Inject;

import static no.ssb.lds.core.utils.FileAndClasspathReaderUtils.readFileOrClasspathResource;
import static org.testng.Assert.assertEquals;

@Listeners(TestServerListener.class)
@Test(singleThreaded = true)
public class SourceTest {

    @Inject
    private TestClient client;

    @Inject
    private TestServer server;

    @Test
    @ConfigurationOverride({
            "txlog.split.sources", "true",
            "txlog.default-source", "default",
            "txlog.rawdata.topic-prefix", "tx-empty-",
            "txlog.rawdata.provider", "memory"
    })
    public void thatLastSourceIdIsNullWhenNoDataHasBeenReceivedForSource() {
        ObjectNode expected = JsonTools.mapper.createObjectNode();
        expected.putNull("lastSourceId");

        String response = client.get("/source/MyEmptySource").expect200Ok().body();
        JsonNode actual = JsonTools.toJsonNode(response);
        assertEquals(actual, expected);
    }

    @Test
    @ConfigurationOverride({
            "txlog.split.sources", "true",
            "txlog.default-source", "default",
            "txlog.rawdata.topic-prefix", "tx-split-",
            "txlog.rawdata.provider", "memory"
    })
    public void thatLastSourceIdIsReturnsLastOfThreeMessages() throws InterruptedException {
        client.put("/data/provisionagreement/2a41c?sync=true&source=A&sourceId=a1", readFileOrClasspathResource("demo/1-sirius.json")).expect201Created();
        client.put("/data/provisionagreement/2a41c/address?sync=true&source=A&sourceId=a2", readFileOrClasspathResource("demo/2-sirius-address.json")).expect200Ok();
        client.put("/data/contact/4b2ef?sync=true&source=B&sourceId=b1", readFileOrClasspathResource("demo/3-skrue.json")).expect201Created();
        client.put("/data/contact/821aa?sync=true&source=B&sourceId=b2", readFileOrClasspathResource("demo/4-donald.json")).expect201Created();
        client.put("/data/provisionagreement/2a41c/contacts/contact/4b2ef?sync=true&source=A&sourceId=a3").expect200Ok();
        client.put("/data/provisionagreement/2a41c/contacts/contact/821aa?sync=true&source=B&sourceId=b3").expect200Ok();
        client.delete("/data/provisionagreement/2a41c/contacts/contact/4b2ef?sync=true&source=B&sourceId=b4").expect200Ok();

        {
            ObjectNode expected = JsonTools.mapper.createObjectNode();
            expected.put("lastSourceId", "a3");
            String response = client.get("/source/A").expect200Ok().body();
            JsonNode actual = JsonTools.toJsonNode(response);
            assertEquals(actual, expected);
        }
        {
            ObjectNode expected = JsonTools.mapper.createObjectNode();
            expected.put("lastSourceId", "b4");
            String response = client.get("/source/B").expect200Ok().body();
            JsonNode actual = JsonTools.toJsonNode(response);
            assertEquals(actual, expected);
        }
    }

    @Test
    @ConfigurationOverride({
            "txlog.split.sources", "false",
            "txlog.default-source", "default",
            "txlog.rawdata.topic-prefix", "tx-merged-",
            "txlog.rawdata.provider", "memory"
    })
    public void thatLastSourceIdIsOnlyReturnedForDefaultSourceWhenTxLogIsNotSplitUp() throws Exception {
        client.put("/data/provisionagreement/2a41c?sync=true&source=A&sourceId=a1", readFileOrClasspathResource("demo/1-sirius.json")).expect201Created();
        client.put("/data/provisionagreement/2a41c/address?sync=true&source=A&sourceId=a2", readFileOrClasspathResource("demo/2-sirius-address.json")).expect200Ok();
        client.put("/data/contact/4b2ef?sync=true&source=B&sourceId=b1", readFileOrClasspathResource("demo/3-skrue.json")).expect201Created();
        client.put("/data/contact/821aa?sync=true&source=B&sourceId=b2", readFileOrClasspathResource("demo/4-donald.json")).expect201Created();
        client.put("/data/provisionagreement/2a41c/contacts/contact/4b2ef?sync=true&source=A&sourceId=a3").expect200Ok();
        client.put("/data/provisionagreement/2a41c/contacts/contact/821aa?sync=true&source=B&sourceId=b3").expect200Ok();
        client.delete("/data/provisionagreement/2a41c/contacts/contact/4b2ef?sync=true&source=B&sourceId=b4").expect200Ok();

        {
            ObjectNode expected = JsonTools.mapper.createObjectNode();
            expected.putNull("lastSourceId");
            String response = client.get("/source/A").expect200Ok().body();
            JsonNode actual = JsonTools.toJsonNode(response);
            assertEquals(actual, expected);
        }
        {
            ObjectNode expected = JsonTools.mapper.createObjectNode();
            expected.putNull("lastSourceId");
            String response = client.get("/source/B").expect200Ok().body();
            JsonNode actual = JsonTools.toJsonNode(response);
            assertEquals(actual, expected);
        }
        {
            ObjectNode expected = JsonTools.mapper.createObjectNode();
            expected.put("lastSourceId", "b4");
            String response = client.get("/source/default").expect200Ok().body();
            JsonNode actual = JsonTools.toJsonNode(response);
            assertEquals(actual, expected);
        }
    }
}
