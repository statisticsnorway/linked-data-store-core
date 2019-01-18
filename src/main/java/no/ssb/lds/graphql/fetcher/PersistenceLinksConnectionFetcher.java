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
import no.ssb.lds.api.persistence.json.JsonPersistence;
import no.ssb.lds.api.persistence.streaming.Fragment;
import no.ssb.lds.api.persistence.streaming.Persistence;
import no.ssb.lds.graphql.fetcher.api.SimplePersistence;
import no.ssb.lds.graphql.fetcher.api.SimplePersistenceImplementation;

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
    private final String path;

    private final String sourceEntityName;

    // Target entity name.
    private final String targetEntityName;

    // Name space
    private final String nameSpace;

    private final SimplePersistence persistence;
    private final Pattern pattern;

    public PersistenceLinksConnectionFetcher(JsonPersistence persistence, String nameSpace, String sourceEntityName, String field, String targetEntityName) {
        this(persistence.getPersistence(), nameSpace, sourceEntityName, field, targetEntityName);
    }

    public PersistenceLinksConnectionFetcher(Persistence persistence, String nameSpace, String sourceEntityName, String field, String targetEntityName) {
        this(new SimplePersistenceImplementation(persistence), nameSpace, sourceEntityName, field, targetEntityName);
    }

    public PersistenceLinksConnectionFetcher(SimplePersistence persistence, String nameSpace, String sourceEntityName, String field, String targetEntityName) {
        this.persistence = Objects.requireNonNull(persistence);
        this.nameSpace = Objects.requireNonNull(nameSpace);
        this.sourceEntityName = Objects.requireNonNull(sourceEntityName);
        this.path = "$." + Objects.requireNonNull(field);
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

            Flow.Publisher<Fragment> fragmentPublisher = persistence.readFragments(tx, parameters.getSnapshot(),
                    nameSpace, sourceEntityName, sourceId);

            // TODO: Refactor the API so that we can ask for a particular path with id.
            AtomicReference<String> first = new AtomicReference<>(null);
            AtomicReference<String> last = new AtomicReference<>(null);

            Flowable<String> idFlowable = fromFlowPublisher(fragmentPublisher)
                    .filter(fragment -> {
                        // Keep fragments of relation field.
                        return fragment.path().startsWith(path);
                    }).map(fragment -> {
                        String id = new String(fragment.value());
                        Matcher matcher = pattern.matcher(id);
                        if (matcher.matches()) {
                            return matcher.group("id");
                        } else {
                            throw new IllegalStateException("invalid relation value: " + id);
                        }
                    }).doOnNext(id -> {
                        if (!first.compareAndSet(null, id)) {
                            last.set(id);
                        }
                    });

            String after = parameters.getAfter();
            if (after != null) {
                idFlowable = idFlowable.filter(id -> {
                    return id.compareTo(after) > 0;
                });
            }

            String before = parameters.getBefore();
            if (before != null) {
                idFlowable = idFlowable.takeWhile(id -> {
                    return id.compareTo(before) < 0;
                });
            }

            if (parameters.getFirst() != null) {
                // Note: using limit here prevents upstream to receive all requests.
                idFlowable = idFlowable.take(parameters.getFirst());
            }
            if (parameters.getLast() != null) {
                idFlowable = idFlowable.takeLast(parameters.getLast());
            }

            // TODO: Find out why we need to use concatMapEager here to avoid "Previous fragment was not published!"
            Flowable<JsonDocument> documentFlowable = idFlowable.concatMapEager(tardetId -> {
                return fromFlowPublisher(persistence.readDocument(tx, parameters.getSnapshot(), nameSpace,
                        targetEntityName, tardetId));
            }, Integer.MAX_VALUE, 1);

            List<Edge<Map<String, Object>>> edges = documentFlowable.map(document -> toEdge(document)).toList()
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

            boolean hasPrevious = !firstEdge.getCursor().getValue().equals(first.get());
            boolean hasNext = !lastEdge.getCursor().getValue().equals(last.get());

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
