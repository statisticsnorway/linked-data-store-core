package no.ssb.lds.graphql.fetcher;

import graphql.relay.Connection;
import graphql.relay.DefaultConnection;
import graphql.relay.DefaultPageInfo;
import graphql.relay.Edge;
import graphql.relay.PageInfo;
import graphql.schema.DataFetchingEnvironment;
import io.reactivex.Flowable;
import no.ssb.lds.api.persistence.Transaction;
import no.ssb.lds.api.persistence.json.JsonDocument;
import no.ssb.lds.api.persistence.reactivex.RxJsonPersistence;
import no.ssb.lds.api.persistence.streaming.Fragment;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static hu.akarnokd.rxjava2.interop.FlowInterop.fromFlowPublisher;

/**
 * A wrapper around {@link PersistenceLinksFetcher} that support relay style connections.
 */
public class PersistenceLinksConnectionFetcher extends ConnectionFetcher<Map<String, Object>> {

    // Field name containing the ids of the target.
    private final String relationField;

    private final String sourceEntityName;

    // Target entity name.
    private final String targetEntityName;

    // Name space
    private final String nameSpace;

    private final RxJsonPersistence persistence;
    private final Pattern pattern;

    public PersistenceLinksConnectionFetcher(RxJsonPersistence persistence, String nameSpace, String sourceEntityName, String field, String targetEntityName) {
        this.persistence = Objects.requireNonNull(persistence);
        this.nameSpace = Objects.requireNonNull(nameSpace);
        this.sourceEntityName = Objects.requireNonNull(sourceEntityName);
        this.relationField = Objects.requireNonNull(field);
        this.targetEntityName = Objects.requireNonNull(targetEntityName);
        this.pattern = Pattern.compile("/" + targetEntityName + "/(?<id>.*)");
    }

    /**
     * Extracts the id from the source object in the environment.
     */
    private static String getIdFromSource(DataFetchingEnvironment environment) {
        Map<String, Object> source = environment.getSource();
        return (String) source.get("id");
    }

    @Override
    Connection<Map<String, Object>> getConnection(DataFetchingEnvironment environment, ConnectionParameters parameters) {
        try (Transaction tx = persistence.createTransaction(true)) {

            String sourceId = getIdFromSource(environment);

            Flowable<JsonDocument> documents = persistence.readLinkedDocuments(tx, parameters.getSnapshot(), nameSpace, sourceEntityName, sourceId,
                    relationField, parameters.getRange());

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
