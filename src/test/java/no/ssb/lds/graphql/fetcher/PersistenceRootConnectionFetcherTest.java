package no.ssb.lds.graphql.fetcher;

import graphql.relay.Connection;
import graphql.relay.Edge;
import no.ssb.lds.api.persistence.DocumentKey;
import no.ssb.lds.api.persistence.Transaction;
import no.ssb.lds.api.persistence.json.JsonDocument;
import no.ssb.lds.api.persistence.reactivex.RxJsonPersistence;
import no.ssb.lds.core.persistence.memory.MemoryInitializer;
import org.json.JSONObject;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.time.ZonedDateTime;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

import static no.ssb.lds.graphql.fetcher.PersistenceLinksConnectionFetcherTest.TestEnvironment;
import static org.assertj.core.api.Assertions.assertThat;

public class PersistenceRootConnectionFetcherTest {

    private PersistenceRootConnectionFetcher connectionFetcher;
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
        connectionFetcher = new PersistenceRootConnectionFetcher(persistence, "ns", "Target");
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
                        null // not required by memory store?
                );
            }
            source.put("targetIds", new LinkedList<>(data.keySet()));
        }


    }

    @Test
    public void testForwardPagination() throws Exception {
        Connection<Map<String, Object>> firstFive = connectionFetcher.get(withArguments(Map.of("first", 5)));

        assertThat(firstFive.getPageInfo().isHasPreviousPage()).isFalse();
        assertThat(firstFive.getPageInfo().isHasNextPage()).isTrue();
        assertThat(firstFive.getEdges()).extracting(Edge::getNode).containsExactlyElementsOf(
                () -> data.values().stream().map(JSONObject::toMap).limit(5).iterator()
        );

        Connection<Map<String, Object>> lastFive = connectionFetcher.get(withArguments(Map.of("first", 5, "after", firstFive.getPageInfo().getEndCursor().getValue())));

        assertThat(lastFive.getPageInfo().isHasPreviousPage()).isTrue();
        assertThat(lastFive.getPageInfo().isHasNextPage()).isFalse();
        assertThat(lastFive.getEdges()).extracting(Edge::getNode).containsExactlyElementsOf(
                () -> data.values().stream().map(JSONObject::toMap).skip(5).iterator()
        );
    }

    @Test
    public void testBackwardPagination() throws Exception {
        Connection<Map<String, Object>> lastFive = connectionFetcher.get(withArguments(Map.of("last", 5)));

        assertThat(lastFive.getPageInfo().isHasPreviousPage()).isTrue();
        assertThat(lastFive.getPageInfo().isHasNextPage()).isFalse();
        assertThat(lastFive.getEdges()).extracting(Edge::getNode).containsExactlyElementsOf(
                () -> data.values().stream().map(JSONObject::toMap).skip(5).iterator()
        );

        Connection<Map<String, Object>> firstFive = connectionFetcher.get(withArguments(Map.of("last", 5, "before", lastFive.getPageInfo().getStartCursor().getValue())));

        assertThat(firstFive.getPageInfo().isHasPreviousPage()).isFalse();
        assertThat(firstFive.getPageInfo().isHasNextPage()).isTrue();
        assertThat(firstFive.getEdges()).extracting(Edge::getNode).containsExactlyElementsOf(
                () -> data.values().stream().map(JSONObject::toMap).limit(5).iterator()
        );
    }

    @Test
    public void testAfter() throws Exception {
        Connection<Map<String, Object>> firstFiveAfter = connectionFetcher.get(
                withArguments(Map.of("first", 5, "after", "target-2")));

        assertThat(firstFiveAfter.getPageInfo().isHasPreviousPage()).isTrue();
        assertThat(firstFiveAfter.getPageInfo().isHasNextPage()).isTrue();
        assertThat(firstFiveAfter.getEdges()).extracting(Edge::getNode).containsExactlyElementsOf(
                () -> data.values().stream().map(JSONObject::toMap).skip(3).limit(5).iterator()
        );
    }

    @Test
    public void testBefore() throws Exception {
        Connection<Map<String, Object>> lastFiveBefore = connectionFetcher.get(
                withArguments(Map.of("last", 5, "before", "target-7")));

        assertThat(lastFiveBefore.getPageInfo().isHasPreviousPage()).isTrue();
        assertThat(lastFiveBefore.getPageInfo().isHasNextPage()).isTrue();
        assertThat(lastFiveBefore.getEdges()).extracting(Edge::getNode).containsExactlyElementsOf(
                () -> data.values().stream().map(JSONObject::toMap).skip(2).limit(5).iterator()
        );
    }

    private TestEnvironment withArguments(Map<String, Object> arguments) {
        return new TestEnvironment(arguments, source, snapshot);
    }

}