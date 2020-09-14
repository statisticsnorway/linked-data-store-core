package no.ssb.lds.graphqlneo4j;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import graphql.ExecutionInput;
import graphql.ExecutionResult;
import graphql.GraphQL;
import graphql.schema.GraphQLSchema;
import io.undertow.attribute.ExchangeAttributes;
import io.undertow.predicate.Predicate;
import io.undertow.predicate.Predicates;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.HeaderMap;
import io.undertow.util.Headers;
import io.undertow.util.HttpString;
import io.undertow.util.StatusCodes;
import no.ssb.lds.api.persistence.json.JsonTools;
import no.ssb.lds.api.persistence.reactivex.RxJsonPersistence;
import no.ssb.lds.graphql.GraphQLUndertowContext;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.driver.summary.ResultSummary;
import org.neo4j.graphql.Cypher;
import org.neo4j.graphql.Translator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Scanner;
import java.util.Set;

import static io.undertow.util.Headers.ALLOW;
import static io.undertow.util.Methods.GET;
import static io.undertow.util.Methods.GET_STRING;
import static io.undertow.util.Methods.OPTIONS;
import static io.undertow.util.Methods.OPTIONS_STRING;
import static io.undertow.util.Methods.POST;
import static io.undertow.util.Methods.POST_STRING;
import static no.ssb.lds.api.persistence.json.JsonTools.mapper;

/**
 * Handler that executes GraphQL queries.
 * <p>
 * Supports GET and POST requests as described on the
 * <a href="https://graphql.org/learn/serving-over-http/">GraphQL website</a>
 */
public class GraphQLNeo4jHttpHandler implements HttpHandler {

    private static final Logger LOG = LoggerFactory.getLogger(GraphQLNeo4jHttpHandler.class);

    private static final Predicate IS_JSON = Predicates.regex(
            ExchangeAttributes.requestHeader(Headers.CONTENT_TYPE),
            "application/(.*\\+)?json"
    );

    private static final Predicate IS_GRAPHQL = Predicates.regex(
            ExchangeAttributes.requestHeader(Headers.CONTENT_TYPE),
            "application/(.*\\+)?graphql"
    );

    private final GraphQLSchema graphQlSchema;
    private final Translator translator;
    private final Set<String> domains;
    private final RxJsonPersistence persistence;

    /**
     * Constructs a handler with the specified GraphQL instance.
     *
     * @param graphQlSchema the graphQl schema.
     * @param persistence
     * @throws NullPointerException if the graphQl was null.
     */
    public GraphQLNeo4jHttpHandler(GraphQLSchema graphQlSchema, Set<String> domains, RxJsonPersistence persistence) {
        this.graphQlSchema = Objects.requireNonNull(graphQlSchema);
        this.translator = new Translator(graphQlSchema);
        this.domains = domains;
        this.persistence = persistence;
    }

    private static Optional<String> extractParam(Map<String, Deque<String>> parameters, String name) {
        Deque<String> parameter = parameters.get(name);
        if (parameter != null && !parameter.isEmpty()) {
            String first = parameter.removeFirst();
            if (!parameter.isEmpty()) {
                throw new IllegalArgumentException(
                        String.format("more than one \"%s\" parameter", name)
                );
            }
            return Optional.of(first);
        }
        return Optional.empty();
    }

    private static JsonNode toJson(HttpServerExchange exchange) throws IOException {
        try (InputStream bi = new BufferedInputStream(exchange.getInputStream())) {
            return mapper.readTree(new InputStreamReader(bi, Charset.forName(exchange.getRequestCharset())));
        }
    }

    private static String toString(HttpServerExchange exchange) throws IOException {
        try (InputStream i = new BufferedInputStream(exchange.getInputStream())) {
            Scanner scanner = new Scanner(i, Charset.forName(exchange.getRequestCharset())).useDelimiter("\\A");
            return scanner.hasNext() ? scanner.next() : "";
        }
    }

    @Override
    public void handleRequest(HttpServerExchange exchange) throws Exception {
        if (exchange.isInIoThread()) {
            exchange.dispatch(this);
            return;
        }

        long startMs = System.currentTimeMillis();

        HttpString method = exchange.getRequestMethod();
        if (method.equals(OPTIONS)) {
            HeaderMap headers = exchange.getResponseHeaders();
            headers.add(ALLOW, GET.toString());
            headers.add(ALLOW, POST_STRING);
            headers.add(ALLOW, GET_STRING);
            headers.add(ALLOW, OPTIONS_STRING);
            return;
        }

        exchange.startBlocking();
        ExecutionInput.Builder executionInputBuilder = ExecutionInput.newExecutionInput();
        if (method.equals(POST)) {
            if (IS_GRAPHQL.resolve(exchange)) {
                executionInputBuilder.query(toString(exchange));
            } else if (IS_JSON.resolve(exchange)) {
                JsonNode json = toJson(exchange);
                executionInputBuilder.query(json.get("query").textValue());
                if (json.has("variables") && !json.get("variables").isNull()) {
                    executionInputBuilder.variables(JsonTools.toMap(json.get("variables")));
                }
                if (json.has("operationName")) {
                    executionInputBuilder.operationName(json.get("operationName").textValue());
                }
            } else {
                exchange.setStatusCode(StatusCodes.UNSUPPORTED_MEDIA_TYPE);
                return;
            }
        } else if (method.equals(GET)) {

            Map<String, Deque<String>> parameters = exchange.getQueryParameters();

            Optional<String> query = extractParam(parameters, "query");
            query.ifPresent(executionInputBuilder::query);

            Optional<String> operationName = extractParam(parameters, "operationName");
            operationName.ifPresent(executionInputBuilder::operationName);

            Optional<String> variables = extractParam(parameters, "variables");
            variables.map(JsonTools::toJsonNode).map(JsonTools::toMap).ifPresent(executionInputBuilder::variables);

        } else {
            exchange.setStatusCode(StatusCodes.METHOD_NOT_ALLOWED);
            return;
        }

        ExecutionInput executionInput = executionInputBuilder.build();

        // Add context.
        executionInputBuilder.context(new GraphQLUndertowContext(exchange, executionInput));

        // Execute

        // Introspection Queries are passed to graphql-java normal execution
        if (executionInput.getQuery().contains("query IntrospectionQuery")) {
            GraphQL graphQL = GraphQL.newGraphQL(graphQlSchema).build();

            // Execute
            ExecutionResult result = graphQL.execute(executionInput);

            // Serialize
            Map<String, Object> resultMap = result.toSpecification();
            String jsonResult = JsonTools.toJson(JsonTools.toJsonNode(resultMap));

            exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");
            exchange.setStatusCode(StatusCodes.OK);
            exchange.getResponseSender().send(jsonResult);
            return;
        }

        ObjectNode jsonBody = new ObjectNode(mapper.getNodeFactory());
        ArrayNode data = jsonBody.putArray("data");
        ObjectNode metadata = jsonBody.putObject("metadata");

        long beforeTranslation = System.currentTimeMillis();

        // translate query to Cypher and execute against neo4j
        ZonedDateTime snapshot = ZonedDateTime.now();
        LOG.debug("GraphQL BEFORE transformation:\n{}\n", executionInput.getQuery());
        String query = GraphQLQueryTransformer.addTimeBasedVersioningArgumentValues(graphQlSchema, executionInput);
        LOG.debug("GraphQL AFTER transformation:\n{}\n", query);
        long afterTransformation = System.currentTimeMillis();
        List<Cypher> cyphers = translator.translate(query, getParamsWithVersionIfMissing(snapshot, executionInput.getVariables()));

        long beforeNeo4j = System.currentTimeMillis();
        Driver driver = persistence.getInstance(Driver.class);
        for (Cypher cypher : cyphers) {
            LOG.debug("{}", cypher.toString());
            LinkedHashMap<String, Object> params = new LinkedHashMap<>(cypher.component2());
            params.putIfAbsent("_version", snapshot);
            List<Map<String, Object>> resultAsMap;
            try (Session session = driver.session()) {
                // TODO: When https://github.com/neo4j/neo4j/issues/12583 is resolved, run query without splitting

                // split query
                CypherQueryProjectionSplitter splitter = new CypherQueryProjectionSplitter(domains);
                splitter.transform(cypher.component1());

                // run selection query
                String selection = splitter.getSelection();
                Result selectionResult = session.run(selection, params, TransactionConfig.builder()
                        .withTimeout(Duration.ofSeconds(10)).build());
                List<Long> idsFromSelection = selectionResult.list(r -> r.get("id").asLong());
                ResultSummary selectionResultSummary = selectionResult.consume();

                // run projection query
                params.put("ids", idsFromSelection);
                String projection = splitter.getProjection();
                Result projectionResult = session.run(projection, params, TransactionConfig.builder()
                        .withTimeout(Duration.ofSeconds(10)).build());
                resultAsMap = projectionResult.list(Record::asMap);
                ResultSummary projectionResultSummary = projectionResult.consume();
            }
            JsonNode jsonNode = JsonTools.toJsonNode(resultAsMap);
            if (jsonNode.isArray()) {
                data.addAll((ArrayNode) jsonNode);
            } else {
                data.add(jsonNode);
            }
        }
        long afterNeo4j = System.currentTimeMillis();

        ObjectNode duration = metadata.putObject("latency");
        duration.put("preparation", (afterNeo4j - startMs) - ((afterNeo4j - beforeNeo4j) + (beforeNeo4j - beforeTranslation)));
        duration.put("graphql-transformation", afterTransformation - beforeTranslation);
        duration.put("graphql-to-cypher", beforeNeo4j - afterTransformation);
        duration.put("neo4j", afterNeo4j - beforeNeo4j);
        duration.put("total", afterNeo4j - startMs);

        // Serialize
        String jsonResult = JsonTools.toJson(jsonBody);

        exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");
        exchange.setStatusCode(StatusCodes.OK);
        exchange.getResponseSender().send(jsonResult);
    }

    private LinkedHashMap<String, Object> getParamsWithVersionIfMissing(ZonedDateTime timeBasedVersion, Map<String, Object> theParams) {
        LinkedHashMap<String, Object> params = new LinkedHashMap<>(theParams);
        params.putIfAbsent("_version", timeBasedVersion);
        return params;
    }
}
