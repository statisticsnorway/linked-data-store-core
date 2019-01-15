package no.ssb.lds.graphql.fetcher;

import graphql.relay.Connection;
import graphql.relay.DefaultConnection;
import graphql.relay.DefaultPageInfo;
import graphql.relay.Edge;
import graphql.relay.PageInfo;
import io.reactivex.Flowable;
import no.ssb.lds.api.persistence.Transaction;
import no.ssb.lds.api.persistence.json.JsonDocument;
import no.ssb.lds.api.persistence.json.JsonPersistence;
import no.ssb.lds.graphql.fetcher.api.SimplePersistence;
import no.ssb.lds.graphql.fetcher.api.SimplePersistenceImplementation;

import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static hu.akarnokd.rxjava2.interop.FlowInterop.fromFlowPublisher;
import static no.ssb.lds.graphql.fetcher.api.SimplePersistence.Range;

/**
 * Root fetcher that supports relay style connections.
 */
public class PersistenceRootConnectionFetcher extends ConnectionFetcher<Map<String, Object>> {


    private final SimplePersistence persistence;
    private String nameSpace;
    private String entityName;

    public PersistenceRootConnectionFetcher(JsonPersistence persistence, String nameSpace, String entityName) {
        this.persistence = new SimplePersistenceImplementation(Objects.requireNonNull(persistence.getPersistence()));
        this.nameSpace = Objects.requireNonNull(nameSpace);
        this.entityName = Objects.requireNonNull(entityName);
    }

    @Override
    Connection<Map<String, Object>> getConnection(ZonedDateTime snapshot, String after, String before, Integer last, Integer first) {
        try (Transaction tx = persistence.createTransaction(true)) {

            Flowable<JsonDocument> documentFlowable = fromFlowPublisher(
                    persistence.readDocuments(tx, snapshot, nameSpace, entityName, Range.between(
                            after,
                            before
                    ))
            );

            if (first != null) {
                documentFlowable = documentFlowable.limit(first);
            }
            if (last != null) {
                documentFlowable = documentFlowable.takeLast(last);
            }

            List<Edge<Map<String, Object>>> edges = documentFlowable.map(document -> toEdge(document)).toList().blockingGet();

            if (edges.isEmpty()) {
                PageInfo pageInfo = new DefaultPageInfo(null, null, false, false);
                return new DefaultConnection<>(Collections.emptyList(), pageInfo);
            }

            Edge<Map<String, Object>> firstEdge = edges.get(0);
            Edge<Map<String, Object>> lastEdge = edges.get(edges.size() - 1);

            boolean hasPrevious = persistence.hasPrevious(tx, snapshot, nameSpace, entityName,
                    firstEdge.getCursor().getValue());

            boolean hasNext = persistence.hasNext(tx, snapshot, nameSpace, entityName,
                    lastEdge.getCursor().getValue());

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
