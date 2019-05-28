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
import no.ssb.lds.api.persistence.reactivex.Range;
import no.ssb.lds.api.persistence.reactivex.RxJsonPersistence;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A fetcher that support relay style connection parameters and type.
 */
public class PersistenceLinksConnectionFetcher extends ConnectionFetcher<Map<String, Object>> {

    // Field name containing the ids of the target.
    private final JsonNavigationPath relationPath;

    private final String sourceEntityName;

    // Target entity name.
    private final String targetEntityName;

    // Name space
    private final String nameSpace;

    private final RxJsonPersistence persistence;

    public PersistenceLinksConnectionFetcher(RxJsonPersistence persistence, String nameSpace, String sourceEntityName, JsonNavigationPath path, String targetEntityName) {
        this.persistence = Objects.requireNonNull(persistence);
        this.nameSpace = Objects.requireNonNull(nameSpace);
        this.sourceEntityName = Objects.requireNonNull(sourceEntityName);
        this.relationPath = Objects.requireNonNull(path);
        this.targetEntityName = Objects.requireNonNull(targetEntityName);
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

            String sourceId = getIdFromSource(environment);

            Flowable<JsonDocument> documents = persistence.readTargetDocuments(tx, parameters.getSnapshot(), nameSpace,
                    sourceEntityName, sourceId, relationPath, targetEntityName, parameters.getRange());

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

            boolean hasPrevious = true;
            if (environment.getSelectionSet().contains("pageInfo/hasPreviousPage")) {
                hasPrevious = persistence.readTargetDocuments(tx, parameters.getSnapshot(), nameSpace, sourceEntityName,
                        sourceId, relationPath, targetEntityName, Range.lastBefore(1, firstEdge.getCursor().getValue())
                ).isEmpty().map(wasEmpty -> !wasEmpty).blockingGet();
            }

            boolean hasNext = true;
            if (environment.getSelectionSet().contains("pageInfo/hasNextPage")) {
                hasNext = persistence.readTargetDocuments(tx, parameters.getSnapshot(), nameSpace, sourceEntityName,
                        sourceId, relationPath, targetEntityName, Range.firstAfter(1, lastEdge.getCursor().getValue())
                ).isEmpty().map(wasEmpty -> !wasEmpty).blockingGet();
            }

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
