package no.ssb.lds.graphql.fetcher;

import graphql.relay.Connection;
import graphql.relay.DefaultConnection;
import graphql.relay.DefaultConnectionCursor;
import graphql.relay.DefaultEdge;
import graphql.relay.DefaultPageInfo;
import graphql.relay.Edge;
import graphql.relay.InvalidPageSizeException;
import graphql.relay.PageInfo;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import no.ssb.lds.api.persistence.Transaction;
import no.ssb.lds.api.persistence.json.JsonDocument;
import no.ssb.lds.api.persistence.json.JsonPersistence;
import no.ssb.lds.graphql.GraphQLContext;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.lang.String.format;

/**
 * Root fetcher that supports relay style connections.
 */
public class PersistenceRootConnectionFetcher implements DataFetcher<Connection<Map<String, Object>>> {

    public static final String AFTER_ARG_NAME = "after";
    private static final String BEFORE_ARG_NAME = "before";
    private static final String FIRST_ARG_NAME = "first";
    private static final String LAST_ARG_NAME = "last";

    private final JsonPersistence persistence;
    private String nameSpace;
    private String entityName;

    public PersistenceRootConnectionFetcher(JsonPersistence persistence, String nameSpace, String entityName) {
        this.persistence = Objects.requireNonNull(persistence);
        this.nameSpace = Objects.requireNonNull(nameSpace);
        this.entityName = Objects.requireNonNull(entityName);
    }

    private static Edge<Map<String, Object>> toEdge(JsonDocument document) {
        return new DefaultEdge<>(
                document.document().toMap(),
                new DefaultConnectionCursor(document.key().id())
        );
    }

    private static String getAfterFrom(DataFetchingEnvironment environment) {
        return environment.getArgument(AFTER_ARG_NAME);
    }

    private static String getBeforeFrom(DataFetchingEnvironment environment) {
        return environment.getArgument(BEFORE_ARG_NAME);
    }

    private static ZonedDateTime getSnapshotFrom(DataFetchingEnvironment environment) {
        GraphQLContext context = environment.getContext();
        return context.getSnapshot();
    }

    private static Integer getLastFrom(DataFetchingEnvironment environment) {
        return environment.getArgument(LAST_ARG_NAME);
    }

    private static Integer getFirstFrom(DataFetchingEnvironment environment) {
        return environment.getArgument(FIRST_ARG_NAME);
    }

    @Override
    public Connection<Map<String, Object>> get(DataFetchingEnvironment environment) throws Exception {
        ZonedDateTime snapshot = getSnapshotFrom(environment);
        Integer first = getFirstFrom(environment);
        Integer last = getLastFrom(environment);

        try (Transaction tx = persistence.createTransaction(true)) {
            // TODO: Refactor when persistence supports after and before parameters.
            Iterable<JsonDocument> completedSlice = persistence.findAll(
                    tx, snapshot, nameSpace, entityName, null, Integer.MAX_VALUE).join();

            Stream<JsonDocument> stream = StreamSupport.stream(completedSlice.spliterator(), false);

            // TODO: Persistence should support this.
            AtomicReference<JsonDocument> firstSeen = new AtomicReference<>(null);
            AtomicReference<JsonDocument> lastSeen = new AtomicReference<>(null);
            stream = stream.peek(document -> {
                if (!firstSeen.compareAndSet(null, document)) {
                    lastSeen.set(document);
                }
            });

            String after = getAfterFrom(environment);
            if (after != null) {
                stream = stream.dropWhile(jsonDocument -> jsonDocument.key().id().compareTo(after) <= 0);
            }
            String before = getBeforeFrom(environment);
            if (before != null) {
                stream = stream.takeWhile(jsonDocument -> jsonDocument.key().id().compareTo(before) < 0);
            }

            List<Edge<Map<String, Object>>> edges = stream.map(document -> toEdge(document))
                    .collect(Collectors.toList());

            if (first != null) {
                if (first < 0) {
                    throw new IllegalArgumentException(format("The page size must not be negative: 'first'=%s", first));
                }
                edges = edges.subList(0, first <= edges.size() ? first : edges.size());
            }
            if (last != null) {
                if (last < 0) {
                    throw new IllegalArgumentException(format("The page size must not be negative: 'last'=%s", last));
                }
                edges = edges.subList(last > edges.size() ? 0 : edges.size() - last, edges.size());
            }

            Edge<Map<String, Object>> firstEdge = edges.get(0);
            Edge<Map<String, Object>> lastEdge = edges.get(edges.size() - 1);

            PageInfo pageInfo = new DefaultPageInfo(
                    firstEdge.getCursor(),
                    lastEdge.getCursor(),
                    !firstEdge.getCursor().equals(toEdge(firstSeen.get()).getCursor()),
                    !lastEdge.getCursor().equals(toEdge(lastSeen.get()).getCursor())
            );

            return new DefaultConnection<>(
                    edges,
                    pageInfo
            );
        }
    }

}
