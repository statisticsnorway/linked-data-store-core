package no.ssb.lds.graphql.fetcher;

import graphql.relay.Connection;
import graphql.relay.DefaultConnection;
import graphql.relay.DefaultPageInfo;
import graphql.relay.Edge;
import graphql.relay.PageInfo;
import graphql.schema.DataFetchingEnvironment;
import io.reactivex.Flowable;
import no.ssb.lds.api.json.JsonNavigationPath;
import no.ssb.lds.api.persistence.DocumentKey;
import no.ssb.lds.api.persistence.Transaction;
import no.ssb.lds.api.persistence.json.JsonDocument;
import no.ssb.lds.api.persistence.reactivex.RxJsonPersistence;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Reverse link fetcher.
 */
public class PersistenceReverseLinksConnectionFetcher extends ConnectionFetcher<Map<String, Object>> {

    // Field name containing the ids of the target.
    private final JsonNavigationPath relationPath;

    private final String sourceEntityName;

    // Target entity name.
    private final String targetEntityName;

    // Name space
    private final String nameSpace;

    private final RxJsonPersistence persistence;

    public PersistenceReverseLinksConnectionFetcher(JsonNavigationPath relationPath, String sourceEntityName, String targetEntityName, String nameSpace, RxJsonPersistence persistence) {
        this.relationPath = relationPath;
        this.sourceEntityName = sourceEntityName;
        this.targetEntityName = targetEntityName;
        this.nameSpace = nameSpace;
        this.persistence = persistence;
    }

    /**
     * Extracts the id from the source object in the environment.
     */
    private static String getIdFromSource(DataFetchingEnvironment environment) {
        Map<String, Object> source = environment.getSource();
        DocumentKey key = (DocumentKey) source.get("__graphql_internal_document_key");
        return key.id();
    }

    @Override
    Connection<Map<String, Object>> getConnection(DataFetchingEnvironment environment, ConnectionParameters parameters) {
        try (Transaction tx = persistence.createTransaction(true)) {

            String targetId = getIdFromSource(environment);

            Flowable<JsonDocument> documents = persistence.readSourceDocuments(tx, parameters.getSnapshot(), nameSpace,
                    targetEntityName, targetId, relationPath, targetEntityName, parameters.getRange());

            List<Edge<Map<String, Object>>> edges = documents.map(document -> toEdge(document)).toList()
                    .blockingGet();

            if (edges.isEmpty()) {
                PageInfo pageInfo = new DefaultPageInfo(null, null, false,
                        false);
                return new DefaultConnection<>(Collections.emptyList(), pageInfo);
            }

            if (parameters.getFirst() != null) {
                edges = edges.subList(0, parameters.getFirst());
            }

            Edge<Map<String, Object>> firstEdge = edges.get(0);
            Edge<Map<String, Object>> lastEdge = edges.get(edges.size() - 1);

            boolean hasPrevious = true; // !firstEdge.getCursor().getValue().equals(first.get());
            boolean hasNext = true; // !lastEdge.getCursor().getValue().equals(last.get());

            PageInfo pageInfo = new DefaultPageInfo(
                    firstEdge.getCursor(),
                    lastEdge.getCursor(),
                    hasPrevious,
                    hasNext
            );

            return new DefaultConnection<>(
                    edges,
                    pageInfo
            );
        }
    }
}
