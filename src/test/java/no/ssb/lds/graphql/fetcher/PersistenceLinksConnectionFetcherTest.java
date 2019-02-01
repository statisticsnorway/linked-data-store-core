package no.ssb.lds.graphql.fetcher;

import graphql.execution.ExecutionContext;
import graphql.execution.ExecutionId;
import graphql.execution.ExecutionStepInfo;
import graphql.language.Field;
import graphql.language.FragmentDefinition;
import graphql.relay.Connection;
import graphql.relay.Edge;
import graphql.schema.DataFetchingEnvironment;
import graphql.schema.DataFetchingFieldSelectionSet;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLOutputType;
import graphql.schema.GraphQLSchema;
import graphql.schema.GraphQLType;
import io.undertow.server.HttpServerExchange;
import no.ssb.lds.api.persistence.DocumentKey;
import no.ssb.lds.api.persistence.Transaction;
import no.ssb.lds.api.persistence.json.JsonDocument;
import no.ssb.lds.api.persistence.reactivex.RxJsonPersistence;
import no.ssb.lds.core.persistence.memory.MemoryInitializer;
import no.ssb.lds.graphql.GraphQLContext;
import org.dataloader.DataLoader;
import org.json.JSONObject;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.time.ZonedDateTime;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

public class PersistenceLinksConnectionFetcherTest {
    private PersistenceLinksConnectionFetcher connectionFetcher;
    private ZonedDateTime snapshot;
    private LinkedHashMap<String, Object> source = new LinkedHashMap<>();
    private Map<String, JSONObject> data = new LinkedHashMap<>();

    /*
     * Pagination in GraphQL ar handled using So called Relay Connections.
     * graphql java comes with a simple implementation
     *
     * For each one to many relationship we need:
     * [FromObject][ToObject]Connection backed by graphql.relay.Connection
     * [FromObject][ToObject]Egde backed by graphql.relay.Edge.
     *
     * Connections also contain graphql.relay.PageInfo
     */

    @BeforeMethod
    public void setUp() {
        RxJsonPersistence persistence = new MemoryInitializer().initialize("ns",
                Map.of("persistence.mem.wait.min", "0",
                        "persistence.mem.wait.max", "0"),
                Set.of("Source", "Target"));
        connectionFetcher = new PersistenceLinksConnectionFetcher(persistence, "ns", "Source", "$.targetIds", "Target");
        snapshot = ZonedDateTime.now();

        // Populate persistence with fake data.
        try (Transaction tx = persistence.createTransaction(false)) {
            for (int i = 0; i < 10; i++) {

                // Save reference for assertions.
                String entityId = String.format("target-%s", i);
                JSONObject jsonObject = new JSONObject();
                jsonObject.put("id", entityId);
                data.put(String.format("/Target/%s", entityId), jsonObject);

                // Put the document into persistence.
                DocumentKey key = new DocumentKey("ns", "Target", entityId, snapshot);
                persistence.createOrOverwrite(
                        tx,
                        new JsonDocument(key, jsonObject),
                        null // not required by memory store.
                );
            }
            source.put("id", "sourceId");
            source.put("targetIds", new LinkedList<>(data.keySet()));
            DocumentKey key = new DocumentKey("ns", "Source", "sourceId", snapshot);
            persistence.createOrOverwrite(
                    tx, new JsonDocument(key, new JSONObject(source)),
                    null // not required by memory store.
            );
        }


    }

    @Test
    public void testForwardPagination() throws Exception {
        Connection<Map<String, Object>> firstFive = connectionFetcher.get(withArguments(Map.of("first", 5)));

        assertThat(firstFive.getPageInfo().isHasPreviousPage())
                .as("hasPreviousPage").isFalse();
        assertThat(firstFive.getPageInfo().isHasNextPage())
                .as("hasNextPage").isTrue();
        assertThat(firstFive.getEdges()).extracting(Edge::getNode)
                .as("returned nodes").containsExactlyElementsOf(
                () -> data.values().stream().map(JSONObject::toMap).limit(5).iterator()
        );

        Connection<Map<String, Object>> lastFive = connectionFetcher.get(withArguments(Map.of("first", 5, "after", firstFive.getPageInfo().getEndCursor().getValue())));

        assertThat(lastFive.getPageInfo().isHasPreviousPage()).as("hasPreviousPage").isTrue();
        assertThat(lastFive.getPageInfo().isHasNextPage()).isFalse();
        assertThat(lastFive.getEdges()).extracting(Edge::getNode).containsExactlyElementsOf(
                () -> data.values().stream().map(JSONObject::toMap).skip(5).iterator()
        );
    }

    @Test
    public void testBackwardPagination() throws Exception {
        Connection<Map<String, Object>> lastFive = connectionFetcher.get(withArguments(Map.of("last", 5)));

        assertThat(lastFive.getPageInfo().isHasPreviousPage())
                .as("hasPreviousPage").isTrue();
        assertThat(lastFive.getPageInfo().isHasNextPage())
                .as("hasNextPage").isFalse();
        assertThat(lastFive.getEdges()).extracting(Edge::getNode).containsExactlyElementsOf(
                () -> data.values().stream().map(JSONObject::toMap).skip(5).iterator()
        );

        Connection<Map<String, Object>> firstFive = connectionFetcher.get(withArguments(Map.of("last", 5, "before", lastFive.getPageInfo().getStartCursor().getValue())));

        assertThat(firstFive.getPageInfo().isHasPreviousPage())
                .as("hasPreviousPage").isFalse();
        assertThat(firstFive.getPageInfo().isHasNextPage())
                .as("hasNextPage").isTrue();
        assertThat(firstFive.getEdges()).extracting(Edge::getNode).containsExactlyElementsOf(
                () -> data.values().stream().map(JSONObject::toMap).limit(5).iterator()
        );
    }

    private TestEnvironment withArguments(Map<String, Object> arguments) {
        return new TestEnvironment(arguments, source, snapshot);
    }

    /**
     * For test. Only the interface is marked public.
     */
    public static final class TestEnvironment implements DataFetchingEnvironment {

        private final Map<String, Object> arguments;
        private final Object source;
        private final ZonedDateTime snapshot;

        public TestEnvironment(
                Map<String, Object> arguments,
                Map<String, Object> source,
                ZonedDateTime snapshot
        ) {
            this.arguments = Objects.requireNonNull(arguments);
            this.source = Objects.requireNonNull(source);
            this.snapshot = Objects.requireNonNull(snapshot);
        }

        @Override
        public <T> T getSource() {
            return (T) source;
        }

        @Override
        public Map<String, Object> getArguments() {
            return arguments;
        }

        @Override
        public boolean containsArgument(String name) {
            return arguments.containsKey(name);
        }

        @Override
        public <T> T getArgument(String name) {
            return (T) arguments.get(name);
        }

        @Override
        public <T> T getContext() {
            return (T) new GraphQLContext() {

                @Override
                public HttpServerExchange getExchange() {
                    return null;
                }

                @Override
                public ZonedDateTime getSnapshot() {
                    return snapshot;
                }
            };
        }

        @Override
        public <T> T getRoot() {
            throw new UnsupportedOperationException();
        }

        @Override
        public GraphQLFieldDefinition getFieldDefinition() {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<Field> getFields() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Field getField() {
            throw new UnsupportedOperationException();
        }

        @Override
        public GraphQLOutputType getFieldType() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ExecutionStepInfo getExecutionStepInfo() {
            throw new UnsupportedOperationException();
        }

        @Override
        public GraphQLType getParentType() {
            throw new UnsupportedOperationException();
        }

        @Override
        public GraphQLSchema getGraphQLSchema() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<String, FragmentDefinition> getFragmentsByName() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ExecutionId getExecutionId() {
            throw new UnsupportedOperationException();
        }

        @Override
        public DataFetchingFieldSelectionSet getSelectionSet() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ExecutionContext getExecutionContext() {
            throw new UnsupportedOperationException();
        }

        @Override
        public <K, V> DataLoader<K, V> getDataLoader(String dataLoaderName) {
            throw new UnsupportedOperationException();
        }
    }
}