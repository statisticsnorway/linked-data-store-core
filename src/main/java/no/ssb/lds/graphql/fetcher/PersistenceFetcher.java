package no.ssb.lds.graphql.fetcher;

import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import no.ssb.lds.api.persistence.Transaction;
import no.ssb.lds.api.persistence.json.JsonDocument;
import no.ssb.lds.api.persistence.json.JsonPersistence;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.concurrent.CompletableFuture;

/**
 * DataFetcher that gets the data from {@link JsonPersistence}.
 */
public class PersistenceFetcher implements DataFetcher<Map<String, Object>> {

    private final JsonPersistence backend;
    private final String nameSpace;
    private final String entity;

    /**
     * Create a new instance
     *
     * @param backend   the backend to use.
     * @param nameSpace the namespace.
     * @param entity    the entity name.
     */
    public PersistenceFetcher(JsonPersistence backend, String nameSpace, String entity) {
        this.backend = Objects.requireNonNull(backend);
        this.nameSpace = Objects.requireNonNull(nameSpace);
        this.entity = Objects.requireNonNull(entity);
    }

    @Override
    public Map<String, Object> get(DataFetchingEnvironment environment) throws Exception {
        // TODO get snapshot timestamp from client through data-fetching-environment
        ZonedDateTime snapshot = ZonedDateTime.now(ZoneId.of("Etc/UTC"));
        JsonDocument document = readDocument(environment.getArgument("id"), snapshot);
        return document != null ? document.document().toMap() : null;
    }

    private JsonDocument readDocument(String id, ZonedDateTime snapshot) {
        try (Transaction tx = backend.createTransaction(true)) {
            CompletableFuture<JsonDocument> future = backend.read(tx, snapshot, nameSpace, this.entity, id);
            return future.join();
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
