package no.ssb.lds.graphql.fetcher;

import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import no.ssb.lds.api.persistence.Transaction;
import no.ssb.lds.api.persistence.json.JsonDocument;
import no.ssb.lds.api.persistence.reactivex.RxJsonPersistence;
import no.ssb.lds.graphql.GraphQLContext;

import java.time.ZonedDateTime;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;

/**
 * DataFetcher that gets the data from {@link RxJsonPersistence}.
 */
public class PersistenceFetcher implements DataFetcher<Map<String, Object>> {

    private final RxJsonPersistence backend;
    private final String nameSpace;
    private final String entity;

    /**
     * Create a new instance
     *
     * @param backend   the backend to use.
     * @param nameSpace the namespace.
     * @param entity    the entity name.
     */
    public PersistenceFetcher(RxJsonPersistence backend, String nameSpace, String entity) {
        this.backend = Objects.requireNonNull(backend);
        this.nameSpace = Objects.requireNonNull(nameSpace);
        this.entity = Objects.requireNonNull(entity);
    }

    @Override
    public Map<String, Object> get(DataFetchingEnvironment environment) throws Exception {
        GraphQLContext context = environment.getContext();
        JsonDocument document = readDocument(environment.getArgument("id"), context.getSnapshot());
        if (document != null) {
            Map<String, Object> map = document.toMap();
            map.put("__graphql_internal_document_key", document.key());
            return map;
        } else {
            return null;
        }
    }

    private JsonDocument readDocument(String id, ZonedDateTime snapshot) {
        try (Transaction tx = backend.createTransaction(true)) {
            return backend.readDocument(tx, snapshot, nameSpace, this.entity, id).blockingGet();
        }
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", PersistenceFetcher.class.getSimpleName() + "[", "]")
                .add("backend=" + backend)
                .add("nameSpace='" + nameSpace + "'")
                .add("entity='" + entity + "'")
                .toString();
    }
}
