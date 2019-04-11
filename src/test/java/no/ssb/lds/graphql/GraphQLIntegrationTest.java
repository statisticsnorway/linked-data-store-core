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
    public void thatGraphQLEndpointSupportsCors() {

        client.options("/graphql",
                "Access-Controll-Request-Method", "POST"
                ).response().headers();
    }

    @Test
    @ConfigurationOverride({
            "graphql.enabled", "true",
            "specification.schema", "spec/demo/contact.json,spec/demo/provisionagreement.json"
    })
    public void thatGetContactOnlyWorks() {
        // setup demo data
        putResource("/data/contact/821aa", "demo/4-donald.json");
        assertEquals(client.get("/data/contact/821aa").expect200Ok().body(), "{\"email\":\"donald@duck.no\",\"name\":\"Donald Duck\"}");

        assertNoErrors(executeGraphQLQuery("spec/demo/graphql/contact_only.json"));
        assertNoErrors(executeGraphQLQuery("spec/demo/graphql/contact_by_id.json", "821aa"));
    }

    @Test
    @ConfigurationOverride({
            "graphql.enabled", "true",
            "specification.schema", "spec/demo/contact.json"
    })
    public void thatSeachByContactNameWorks() {
        // setup demo data
        putResource("/data/contact/821aa", "demo/4-donald.json");
        JsonNode result = executeGraphQLQuery("spec/demo/graphql/search_contact.json", "Duck");
        assertNoErrors(result);
        Assert.assertEquals(result.get("data").get("Search").get("edges").size(), 1);
        Assert.assertEquals(result.get("data").get("Search").get("edges").get(0).get("node").get("name").textValue(), "Donald Duck");
        // Check that the entity is deleted from the index
        client.delete("/data/contact/821aa?sync=true");
        result = executeGraphQLQuery("spec/demo/graphql/search_contact.json", "Duck");
        Assert.assertEquals(result.get("data").get("Search").get("edges").size(), 0);
    }

    @Test
    @ConfigurationOverride({
            "graphql.enabled", "true",
            "specification.schema", "spec/demo/contact.json,spec/demo/provisionagreement.json"
    })
    public void thatNestedSeachWorks() {
        // setup demo data
        putResource("/data/provisionagreement/2a41c", "demo/1-sirius.json");
        putResource("/data/provisionagreement/2a41c/address", "demo/2-sirius-address.json");

        JsonNode result = executeGraphQLQuery("spec/demo/graphql/search_address.json", "Andeby");
        assertNoErrors(result);
        Assert.assertEquals(result.get("data").get("Search").get("edges").size(), 1);
        Assert.assertEquals(result.get("data").get("Search").get("edges").get(0).get("node").get("name").textValue(), "Sirius");
        // Check that the entity index is updated
        client.put("/data/provisionagreement/2a41c", FileAndClasspathReaderUtils.readFileOrClasspathResource(
                "demo/1-sirius.json").replace("Sirius", "Jupiter"));
        result = executeGraphQLQuery("spec/demo/graphql/search_address.json", "Jupiter");
        assertNoErrors(result);
        Assert.assertEquals(result.get("data").get("Search").get("edges").size(), 1);
    }

    @Test
    @ConfigurationOverride({
            "graphql.enabled", "true",
            "specification.schema", "spec/demo/contact.json,spec/demo/provisionagreement.json"
    })
    public void thatLinkingQueryWorks() {
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
        client.put(path + "?sync=true", FileAndClasspathReaderUtils.readFileOrClasspathResource(resourceFilePath));
    }

    private void assertNoErrors(JsonNode responseRootNode) {
        System.out.println(responseRootNode);
        if (responseRootNode.has("errors")) {
            // there should not be any errors!
            JsonNode errors = responseRootNode.get("errors");
            String errorMessages = JsonTools.toJson(errors);
            Assert.fail(errorMessages);
        }
    }

    private JsonNode executeGraphQLQuery(String path, Object... params) {
        String query = String.format(FileAndClasspathReaderUtils.readFileOrClasspathResource(path), params);
        return JsonTools.toJsonNode(client.postJson("/graphql", query)
                .expect200Ok()
                .body());
    }
}
