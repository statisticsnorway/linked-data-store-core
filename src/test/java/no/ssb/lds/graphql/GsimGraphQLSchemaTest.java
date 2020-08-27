package no.ssb.lds.graphql;

import com.fasterxml.jackson.databind.JsonNode;
import no.ssb.lds.api.persistence.DocumentKey;
import no.ssb.lds.api.persistence.Transaction;
import no.ssb.lds.api.persistence.json.JsonDocument;
import no.ssb.lds.api.persistence.json.JsonTools;
import no.ssb.lds.api.persistence.reactivex.RxJsonPersistence;
import no.ssb.lds.core.utils.FileAndClasspathReaderUtils;
import no.ssb.lds.test.ConfigurationOverride;
import no.ssb.lds.test.client.TestClient;
import no.ssb.lds.test.server.TestServer;
import no.ssb.lds.test.server.TestServerListener;
import org.json.JSONObject;
import org.skyscreamer.jsonassert.JSONAssert;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import javax.inject.Inject;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZoneId;
import java.time.ZonedDateTime;

@Listeners(TestServerListener.class)
public class GsimGraphQLSchemaTest {

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
    @ConfigurationOverride({
            "graphql.schema", "src/test/resources/gsim/schema.graphql"
    })
    public void thatAllGsimExamplesAreStoredAndRetainedIntact() throws IOException {
        Files.list(Path.of("src/test/resources/gsim/examples")).forEach(path -> {
            try {
                String body = FileAndClasspathReaderUtils.readFileAsUtf8(path.toString());
                String domain = path.getFileName().toString().substring(0, path.getFileName().toString().indexOf("_"));
                JSONObject document = new JSONObject(body);
                String id = document.getString("id");
                client.put(String.format("/data/%s/%s?sync=true", domain, id), body).expectAnyOf(200, 201);
                String actual = client.get(String.format("/data/%s/%s", domain, id)).expect200Ok().body();
                JSONAssert.assertEquals(body, actual, false);
            } catch (Throwable e) {
                System.out.printf("ERROR while processing path: %s%n", path.toString());
                throw e;
            }
        });
    }
}
