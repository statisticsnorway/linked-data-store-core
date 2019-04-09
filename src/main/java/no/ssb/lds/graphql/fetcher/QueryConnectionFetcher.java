package no.ssb.lds.graphql.fetcher;

import graphql.relay.Connection;
import graphql.relay.ConnectionCursor;
import graphql.relay.DefaultConnection;
import graphql.relay.DefaultEdge;
import graphql.relay.DefaultPageInfo;
import graphql.relay.Edge;
import graphql.relay.PageInfo;
import graphql.schema.DataFetchingEnvironment;
import io.reactivex.Maybe;
import no.ssb.lds.api.persistence.DocumentKey;
import no.ssb.lds.api.persistence.Transaction;
import no.ssb.lds.api.persistence.json.JsonDocument;
import no.ssb.lds.api.persistence.reactivex.Range;
import no.ssb.lds.api.persistence.reactivex.RxJsonPersistence;
import no.ssb.lds.api.search.SearchIndex;
import no.ssb.lds.api.search.SearchResponse;
import no.ssb.lds.graphql.GraphQLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.StringJoiner;

/**
 * ConnectionFetcher that gets the data from {@link SearchIndex}.
 */
public class QueryConnectionFetcher extends ConnectionFetcher<Map<String, Object>> {

    private static final Logger LOG = LoggerFactory.getLogger(QueryConnectionFetcher.class);
    private static final int MAX_SEARCH_LIMIT = 10;
    private final SearchIndex searchIndex;
    private final RxJsonPersistence persistence;
    private final String nameSpace;
    private final String entity;

    /**
     * Create a new instance
     *
     * @param searchIndex the searchIndex to use.
     * @param persistence the persistence provider to use
     * @param nameSpace   the namespace.
     * @param entity      the entity name.
     */
    public QueryConnectionFetcher(SearchIndex searchIndex, RxJsonPersistence persistence, String nameSpace, String entity) {
        this.searchIndex = Objects.requireNonNull(searchIndex);
        this.persistence = Objects.requireNonNull(persistence);
        this.nameSpace = Objects.requireNonNull(nameSpace);
        this.entity = Objects.requireNonNull(entity);
    }

    @Override
    Connection<Map<String, Object>> getConnection(DataFetchingEnvironment environment, ConnectionParameters connectionParameters) {
        GraphQLContext context = environment.getContext();
        return search(environment.getArgument("query"), environment.getArgument("filter"), context.getSnapshot(),
                connectionParameters.getRange());
    }

    /**
     * Internal class for converting a {@link Range} to index based pagination.
     */
    static class IndexBasedRange {
        private final long from;
        private final long size;

        public IndexBasedRange(long from, long size) {
            this.from = from;
            this.size = size;
        }

        public long getFrom() {
            return from;
        }

        public long getSize() {
            return size;
        }

        /**
         * Converts from a relay style range object to an index based pagination.
         *
         * @param range the relay style range object
         * @param maxSize maximum size of the edges (may or may not be used depending on the type of range).
         * @return this class
         */
        static IndexBasedRange fromRange(Range<String> range, long maxSize) {
            if (range.hasFirst()) {
                return new IndexBasedRange(range.hasAfter() ? getIndex(range.getAfter()) : 0,
                        range.getFirst());
            } else if (range.hasLast()) {
                long end = range.hasBefore() ? getIndex(range.getBefore()) : maxSize;
                return new IndexBasedRange(end - range.getLast(), range.getLast());
            } else {
                long from = range.hasAfter() ? getIndex(range.getAfter()) : 0;
                long to = range.hasBefore() ? getIndex(range.getBefore()) : maxSize;
                return new IndexBasedRange(from, to - from);
            }
        }

        static long getIndex(String cursorValue) {
            return QueryConnectionCursor.fromValue(cursorValue).getIndex();
        }

    }

    private Connection<Map<String, Object>> search(String query, List<String> typeFilter,
                                                   ZonedDateTime snapshot, Range<String> range) {
        IndexBasedRange settings = IndexBasedRange.fromRange(range, MAX_SEARCH_LIMIT);
        HashSet<String> filter = typeFilter != null ? new HashSet<>(typeFilter) : null;
        SearchResponse response = searchIndex.search(query, filter, settings.from, settings.size).blockingGet();

        LOG.info("Search query '{}' resulted in {} hits from search settings. Fetching results from {} to {}", query,
                response.getTotalHits(), settings.from, settings.from + settings.size);
        List<Edge<Map<String, Object>>> edges = response.getResults().stream()
                .map(result -> readDocument(result.getDocumentKey(), snapshot))
                .filter(jsonDocument -> jsonDocument.isPresent())
                .collect(ArrayList::new, (list, document) ->
                        list.add(toEdge(document.get(), new QueryConnectionCursor(settings.from + list.size()))),
                        ArrayList::addAll);

        if (edges.isEmpty()) {
            LOG.info("Search query resulted in 0 documents.", query);
            PageInfo pageInfo = new DefaultPageInfo(null, null, false, false);
            return new DefaultConnection<>(Collections.emptyList(), pageInfo);
        }

        Edge<Map<String, Object>> firstEdge = edges.get(0);
        Edge<Map<String, Object>> lastEdge = edges.get(edges.size() - 1);

        boolean hasPrevious = settings.from > 0;
        boolean hasNext = settings.from + settings.size < response.getTotalHits();

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

    private static Edge<Map<String, Object>> toEdge(JsonDocument document, ConnectionCursor connectionCursor) {
        Map<String, Object> map = document.toMap();
        map.put("__graphql_internal_document_key", document.key());
        return new DefaultEdge<>(map, connectionCursor);
    }

    private Optional<JsonDocument> readDocument(DocumentKey documentKey, ZonedDateTime snapshot) {
        try (Transaction tx = persistence.createTransaction(true)) {
            Maybe<JsonDocument> jsonDocumentMaybe = persistence.readDocument(tx, snapshot, documentKey.namespace(),
                    documentKey.entity(), documentKey.id());
            JsonDocument jsonDocument = jsonDocumentMaybe.blockingGet();
            if (jsonDocument == null) {
                LOG.error("Cound not find document for key {}", documentKey);
                return Optional.empty();
            }
            return Optional.of(jsonDocument);
        }
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", QueryConnectionFetcher.class.getSimpleName() + "[", "]")
                .add("searchIndex=" + searchIndex)
                .add("nameSpace='" + nameSpace + "'")
                .add("entity='" + entity + "'")
                .toString();
    }
}
