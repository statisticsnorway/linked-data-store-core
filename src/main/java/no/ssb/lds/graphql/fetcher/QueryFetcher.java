package no.ssb.lds.graphql.fetcher;

import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.reactivex.Maybe;
import no.ssb.lds.api.persistence.DocumentKey;
import no.ssb.lds.api.persistence.Transaction;
import no.ssb.lds.api.persistence.json.JsonDocument;
import no.ssb.lds.api.persistence.json.JsonTools;
import no.ssb.lds.api.persistence.reactivex.RxJsonPersistence;
import no.ssb.lds.api.search.SearchIndex;
import no.ssb.lds.graphql.GraphQLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.StringJoiner;

/**
 * DataFetcher that gets the data from {@link SearchIndex}.
 */
public class QueryFetcher implements DataFetcher<List<Map<String, Object>>> {

    private static final Logger LOG = LoggerFactory.getLogger(QueryFetcher.class);
    private final SearchIndex searchIndex;
    private final RxJsonPersistence persistence;
    private final String nameSpace;
    private final String entity;

    /**
     * Create a new instance
     *
     * @param searchIndex   the searchIndex to use.
     * @param persistence   the persistence provider to use
     * @param nameSpace     the namespace.
     * @param entity        the entity name.
     */
    public QueryFetcher(SearchIndex searchIndex, RxJsonPersistence persistence, String nameSpace, String entity) {
        this.searchIndex = Objects.requireNonNull(searchIndex);
        this.persistence = Objects.requireNonNull(persistence);
        this.nameSpace = Objects.requireNonNull(nameSpace);
        this.entity = Objects.requireNonNull(entity);
    }

    @Override
    public List<Map<String, Object>> get(DataFetchingEnvironment environment) {
        GraphQLContext context = environment.getContext();
        return search(environment.getArgument("query"), context.getSnapshot());
    }

    private List<Map<String, Object>> search(String query, ZonedDateTime snapshot) {
        return searchIndex.search(query, 0, 100)
                .map(result -> readDocument(result.getDocumentKey(), snapshot))
                .filter(jsonDocument -> jsonDocument.isPresent())
                .map(jsonDocument -> {
                        Map<String, Object> map = JsonTools.toMap(jsonDocument.get().jackson());
                        map.put("__graphql_internal_document_key", jsonDocument.get().key());
                        return map;
                    }
                ).toList().blockingGet();
    }

    private Optional<JsonDocument> readDocument(DocumentKey documentKey, ZonedDateTime snapshot) {
        try (Transaction tx = persistence.createTransaction(true)) {
            Maybe<JsonDocument> jsonDocumentMaybe = persistence.readDocument(tx, snapshot, documentKey.namespace(),
                    documentKey.entity(), documentKey.id());
            JsonDocument jsonDocument = jsonDocumentMaybe.blockingGet();
            if (jsonDocument == null) {
                LOG.error("Cound not find document for key", documentKey);
                return Optional.empty();
            }
            return Optional.of(jsonDocument);
        }
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", QueryFetcher.class.getSimpleName() + "[", "]")
                .add("searchIndex=" + searchIndex)
                .add("nameSpace='" + nameSpace + "'")
                .add("entity='" + entity + "'")
                .toString();
    }
}
