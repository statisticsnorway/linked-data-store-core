package no.ssb.lds.graphql;

import com.fasterxml.jackson.databind.JsonNode;
import no.ssb.lds.api.persistence.json.JsonTools;
import no.ssb.lds.core.utils.FileAndClasspathReaderUtils;
import no.ssb.lds.test.ConfigurationOverride;
import no.ssb.lds.test.client.TestClient;
import no.ssb.lds.test.server.TestServerListener;
import org.testng.Assert;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import javax.inject.Inject;
import java.io.IOException;

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
    public void thatGetContactOnlyWorks() throws IOException {
        // setup demo data
        putResource("/data/contact/821aa", "demo/4-donald.json");
        assertEquals(client.get("/data/contact/821aa").expect200Ok().body(), "{\"email\":\"donald@duck.no\",\"name\":\"Donald Duck\"}");

        assertNoErrors(executeGraphQLQuery("spec/demo/graphql/contact_only.json"));
    }

    @Test
    @ConfigurationOverride({
            "graphql.enabled", "true",
            "specification.schema", "spec/demo/contact.json,spec/demo/provisionagreement.json"
    })
    public void thatLinkingQueryWorks() throws IOException {
        // setup demo data
        putResource("/data/provisionagreement/2a41c", "demo/1-sirius.json");
        putResource("/data/provisionagreement/2a41c/address", "demo/2-sirius-address.json");
        putResource("/data/contact/4b2ef", "demo/3-skrue.json");
        putResource("/data/contact/821aa", "demo/4-donald.json");
        client.put("/data/provisionagreement/2a41c/contacts/contact/4b2ef");
        client.put("/data/provisionagreement/2a41c/contacts/contact/821aa");

        assertNoErrors(executeGraphQLQuery("spec/demo/graphql/basic_query.json"));
    }

    private void putResource(String path, String resourceFilePath) {
        client.put(path, FileAndClasspathReaderUtils.readFileOrClasspathResource(resourceFilePath));
    }

    private void assertNoErrors(String graphqlResponseBody) {
        JsonNode responseRootNode = JsonTools.toJsonNode(graphqlResponseBody);
        if (responseRootNode.has("errors")) {
            // there should not be any errors!
            JsonNode errors = responseRootNode.get("errors");
            String errorMessages = JsonTools.toJson(errors);
            Assert.fail(errorMessages);
        }
    }

    private String executeGraphQLQuery(String path) {
        String query = FileAndClasspathReaderUtils.readFileOrClasspathResource(path);
        return client.postJson("/graphql", query)
                .expect200Ok()
                .body();
    }
}
