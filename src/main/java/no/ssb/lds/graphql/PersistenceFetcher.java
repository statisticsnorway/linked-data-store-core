package no.ssb.lds.graphql;

import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import no.ssb.lds.api.persistence.Persistence;
import no.ssb.lds.api.persistence.Transaction;
import no.ssb.lds.api.persistence.buffered.BufferedPersistence;
import no.ssb.lds.api.persistence.buffered.Document;
import no.ssb.lds.api.persistence.buffered.DocumentIterator;
import no.ssb.lds.core.buffered.DocumentToJson;
import org.json.JSONObject;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.concurrent.CompletableFuture;

/**
 * DataFetcher that gets the data from {@link Persistence}.
 */
public class PersistenceFetcher implements DataFetcher<Map<String, Object>> {

    private final BufferedPersistence backend;
    private final String nameSpace;
    private final String entity;

    /**
     * Create a new instance
     *
     * @param backend   the backend to use.
     * @param nameSpace the namespace.
     * @param entity    the entity name.
     */
    public PersistenceFetcher(BufferedPersistence backend, String nameSpace, String entity) {
        this.backend = Objects.requireNonNull(backend);
        this.nameSpace = Objects.requireNonNull(nameSpace);
        this.entity = Objects.requireNonNull(entity);
    }

    @Override
    public Map<String, Object> get(DataFetchingEnvironment environment) throws Exception {
        // TODO get snapshot timestamp from client through data-fetching-environment
        ZonedDateTime snapshot = ZonedDateTime.now(ZoneId.of("Etc/UTC"));
        JSONObject entity = readDocument(environment.getArgument("id"), snapshot);
        return entity.toMap();
    }

    private JSONObject readDocument(String id, ZonedDateTime snapshot) {
        Document document;
        try (Transaction tx = backend.createTransaction(true)) {
            CompletableFuture<DocumentIterator> future = backend.read(tx, snapshot, nameSpace, this.entity, id);
            DocumentIterator iterator = future.join();
            if (!iterator.hasNext()) {
                return null;
            }
            document = iterator.next();
        }
        return new DocumentToJson(document).toJSONObject();
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
