package no.ssb.lds.graphql;

import com.damnhandy.uri.template.UriTemplate;
import graphql.GraphQL;
import graphql.schema.GraphQLSchema;
import graphql.schema.StaticDataFetcher;
import graphql.schema.idl.RuntimeWiring;
import graphql.schema.idl.SchemaGenerator;
import graphql.schema.idl.SchemaParser;
import graphql.schema.idl.TypeDefinitionRegistry;
import io.undertow.Undertow;
import org.json.JSONObject;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.ProxySelector;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;

import static graphql.schema.idl.RuntimeWiring.newRuntimeWiring;
import static org.testng.Assert.assertEquals;

public class GraphqlHandlerTest {

    private GraphQL graphql;
    private Undertow server;
    private HttpClient client;
    private UriTemplate uriTemplate;

    /**
     * Find a free port for the undertow server.
     */
    private static int findFree() {
        int port = 0;
        while (port == 0) {
            ServerSocket socket = null;
            try {
                socket = new ServerSocket(0);
                port = socket.getLocalPort();
            } catch (IOException e) {
                throw new RuntimeException("Failed finding free port", e);
            } finally {
                try {
                    if (socket != null) socket.close();
                } catch (IOException ignore) {
                }
            }
        }
        return port;
    }

    @BeforeMethod
    public void setUp() {

        // Test client.
        client = HttpClient.newBuilder().version(HttpClient.Version.HTTP_2)
                .followRedirects(HttpClient.Redirect.NORMAL).proxy(ProxySelector.getDefault()).build();

        // Build a fake GraphQL instance.
        String schema = "" +
                "type User {" +
                "   name: String," +
                "   address: Address" +
                "}" +
                "type Address {" +
                "   street: String " +
                "}" +
                "type Query{" +
                "   me: User" +
                "}";

        SchemaParser schemaParser = new SchemaParser();
        TypeDefinitionRegistry typeDefinitionRegistry = schemaParser.parse(schema);

        RuntimeWiring runtimeWiring = newRuntimeWiring()
                .type("Query", builder -> builder.dataFetcher("me", new StaticDataFetcher(
                        Map.of("name", "Hadrien", "address", Map.of("street", "rue de la paix"))
                )))
                .build();

        SchemaGenerator schemaGenerator = new SchemaGenerator();
        GraphQLSchema graphQLSchema = schemaGenerator.makeExecutableSchema(typeDefinitionRegistry, runtimeWiring);

        graphql = GraphQL.newGraphQL(graphQLSchema).build();

        int port = findFree();

        // Test server.
        server = Undertow.builder().addHttpListener(port, "localhost", new GraphqlHandler(graphql)).build();
        server.start();

        // Template for the client.
        uriTemplate = UriTemplate.buildFromTemplate("http://localhost:" + port)
                .query("query", "operationName", "variables").build();
    }

    @AfterMethod
    public void tearDown() {
        server.stop();
    }

    @Test
    public void testGetQueryParameter() throws Exception {

        // When receiving an HTTP GET request, the GraphQL query should be
        // specified in the "query" query string.

        // http://myapi/graphql?query={me{name}}
        HttpRequest request = HttpRequest.newBuilder(
                URI.create(uriTemplate.set("query", "{me{name}}").expand())).build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(response.statusCode(), 200);
        assertEquals(response.body(), "{\"data\":{\"me\":{\"name\":\"Hadrien\"}}}");

    }

    @Test
    public void testGetVariablesParameter() throws IOException, InterruptedException {
        // Query variables can be sent as a JSON-encoded string in an additional
        // query parameter called variables

        HttpRequest request = HttpRequest.newBuilder(
                URI.create(uriTemplate
                        .set("query", "" +
                                "query Test($foo: Boolean!){" +
                                "   me {" +
                                "       name, " +
                                "       address @skip(if: $foo) {" +
                                "           street" +
                                "       }" +
                                "   }" +
                                "}")
                        .set("variables", "{\"foo\":true}")
                        .expand())
        ).build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(response.statusCode(), 200);
        assertEquals(response.body(), "{\"data\":{\"me\":{\"name\":\"Hadrien\"}}}");
    }

    @Test
    public void testGetOperationNameParameter() throws IOException, InterruptedException {

        // If the query contains several named operations, an operationName query
        // parameter can be used to control which one should be executed.

        HttpRequest request = HttpRequest.newBuilder(
                URI.create(uriTemplate
                        .set("query", "" +
                                "query Foo {" +
                                "   me {" +
                                "       name" +
                                "   }" +
                                "}" +
                                "query Bar {" +
                                "   me {" +
                                "       address {" +
                                "           street" +
                                "       }" +
                                "   }" +
                                "}"
                        )
                        .set("operationName", "Bar")
                        .expand())
        ).build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(response.statusCode(), 200);
        assertEquals(response.body(), "{\"data\":{\"me\":{\"address\":{\"street\":\"rue de la paix\"}}}}");

    }

    @Test
    public void testPostValidateContentType() throws IOException, InterruptedException {

        // A standard GraphQL POST request should use the application/json content
        // type, and include a JSON-encoded body.
        // {
        //  "query": "...",
        //  "operationName": "...",
        //  "variables": { "myVariable": "someValue", ... }
        //}
        //
        // operationName and variables are optional fields. operationName is only required
        // if multiple operations are present in the query.

        JSONObject query = new JSONObject(Map.of(
                "query", "query Foo {" +
                        "   me {" +
                        "       name" +
                        "   }" +
                        "}" +
                        "query Bar($foo: Boolean!) {" +
                        "   me {" +
                        "       name" +
                        "       address @skip(if: $foo) {" +
                        "           street" +
                        "       }" +
                        "   }" +
                        "}",
                "operationName", "Bar",
                "variables", Map.of("foo", true)
        ));

        HttpRequest request = HttpRequest.newBuilder(URI.create(uriTemplate.expand()))
                .POST(HttpRequest.BodyPublishers.ofString(query.toString()))
                .header("Content-Type", "application/json")
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(response.statusCode(), 200);
        assertEquals(response.body(), "{\"data\":{\"me\":{\"name\":\"Hadrien\"}}}");

    }
}