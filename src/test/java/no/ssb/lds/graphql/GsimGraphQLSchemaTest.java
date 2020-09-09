package no.ssb.lds.graphql;

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

@Listeners(TestServerListener.class)
public class GsimGraphQLSchemaTest {

    @Inject
    private TestClient client;

    @Inject
    private TestServer server;

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

    @Test
    @ConfigurationOverride({
            "graphql.schema", "src/test/resources/gsim/schema.graphql"
    })
    public void thatSingleGsimExamplesAreStoredAndRetainedIntact() throws IOException {
        Path path = Path.of("src/test/resources/gsim/examples/DataResource_PersonsFamily.json");
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
    }
}
