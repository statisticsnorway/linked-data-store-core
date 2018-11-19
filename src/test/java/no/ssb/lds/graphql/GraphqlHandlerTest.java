package no.ssb.lds.graphql;

import graphql.ExecutionResult;
import graphql.GraphQL;
import graphql.schema.GraphQLSchema;
import graphql.schema.StaticDataFetcher;
import graphql.schema.idl.RuntimeWiring;
import graphql.schema.idl.SchemaGenerator;
import graphql.schema.idl.SchemaParser;
import graphql.schema.idl.TypeDefinitionRegistry;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Methods;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Map;

import static graphql.schema.idl.RuntimeWiring.newRuntimeWiring;

public class GraphqlHandlerTest {

    private GraphQL graphql;

    @BeforeMethod
    public void setUp() {
        // Build a fake GraphQL instance.
        String schema = "type User { name: String } type Query{me: User}";
        SchemaParser schemaParser = new SchemaParser();
        TypeDefinitionRegistry typeDefinitionRegistry = schemaParser.parse(schema);

        RuntimeWiring runtimeWiring = newRuntimeWiring()
                .type("Query", builder -> builder.dataFetcher("me", new StaticDataFetcher(
                        Map.of("name", "Hadrien")
                )))
                .build();

        SchemaGenerator schemaGenerator = new SchemaGenerator();
        GraphQLSchema graphQLSchema = schemaGenerator.makeExecutableSchema(typeDefinitionRegistry, runtimeWiring);

        graphql = GraphQL.newGraphQL(graphQLSchema).build();
    }

    @Test
    public void testGetQueryParameter() throws Exception {

        // When receiving an HTTP GET request, the GraphQL query should be
        // specified in the "query" query string.

        HttpServerExchange exchange = new HttpServerExchange(null);
        exchange.setRequestMethod(Methods.GET);
        exchange.setQueryString("query={me{name}}");

        GraphqlHandler handler = new GraphqlHandler(graphql);

        handler.handleRequest(exchange);

        // http://myapi/graphql?query={me{name}}
    }

    @Test
    public void testGetVariablesParameter() {
        // Query variables can be sent as a JSON-encoded string in an additional
        // query parameter called variables
    }

    @Test
    public void testGetOperationNameParameter() {
        // If the query contains several named operations, an operationName query
        // parameter can be used to control which one should be executed.
    }

    @Test
    public void testPostValidateContentType() {
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
    }

    @Test
    public void testNoErrorInResponse() {
        // If there were no errors returned, the "errors" field should not be present on the response.
    }

    @Test
    public void testEmptyDataWithErrorInResponse() {
        // If there were no errors returned, the "errors" field should not be present on the response.
    }

    @Test
    public void testNoDataWithoutErrorInResponse() {
        // If no data is returned, according to the GraphQL spec, the "data" field should only be
        // included if the error occurred during execution.
    }
}