package no.ssb.lds.graphql;

import com.fasterxml.jackson.databind.JsonNode;
import no.ssb.lds.core.utils.FileAndClasspathReaderUtils;
import no.ssb.lds.test.ConfigurationOverride;
import no.ssb.lds.test.client.TestClient;
import no.ssb.lds.test.server.TestServerListener;
import org.testng.Assert;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import javax.inject.Inject;
import java.io.IOException;

import static no.ssb.lds.api.persistence.json.JsonDocument.mapper;
import static org.testng.Assert.assertEquals;

@Listeners(TestServerListener.class)
public class GraphQLIntegrationTest {

    @Inject
    TestClient client;

    @Test
    @ConfigurationOverride({
            "graphql.enabled", "true",
            "specification.schema", "spec/demo/contact.json,spec/demo/provisionagreement.json"
    })
    public void thatBasicQueryWorks() throws IOException {
        // setup demo data
        client.put("/data/provisionagreement/2a41c", FileAndClasspathReaderUtils.readFileOrClasspathResource("demo/1-sirius.json"));
        client.put("/data/provisionagreement/2a41c/address", FileAndClasspathReaderUtils.readFileOrClasspathResource("demo/2-sirius-address.json"));
        client.put("/data/contact/4b2ef", FileAndClasspathReaderUtils.readFileOrClasspathResource("demo/3-skrue.json"));
        client.put("/data/contact/821aa", FileAndClasspathReaderUtils.readFileOrClasspathResource("demo/4-donald.json"));
        client.put("/data/provisionagreement/2a41c/contacts/contact/4b2ef");
        client.put("/data/provisionagreement/2a41c/contacts/contact/821aa");
        assertEquals(client.get("/data/provisionagreement/2a41c").expect200Ok().body(), "{\"address\":{\"city\":\"Andeby\",\"street\":\"Pengebingen\"},\"contacts\":[\"/contact/4b2ef\",\"/contact/821aa\"],\"name\":\"Sirius\"}");
        assertEquals(client.get("/data/contact/4b2ef").expect200Ok().body(), "{\"email\":\"skrue@bingen.no\",\"name\":\"Onkel Skrue\"}");
        assertEquals(client.get("/data/contact/821aa").expect200Ok().body(), "{\"email\":\"donald@duck.no\",\"name\":\"Donald Duck\"}");

        // Test a very basic query that gets all data starting with provisionagreement
        String query = FileAndClasspathReaderUtils.readFileOrClasspathResource("spec/demo/graphql/basic_query.json");
        String graphqlResponseBody = client.postJson("/graphql", query)
                .expect200Ok()
                .body();

        JsonNode responseRootNode = mapper.readTree(graphqlResponseBody);
        if (responseRootNode.has("errors")) {
            // there should not be any errors!
            JsonNode errors = responseRootNode.get("errors");
            String errorMessages = mapper.writeValueAsString(errors);
            Assert.fail(errorMessages);
        }
    }
}
