package no.ssb.lds.graphql;

import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import no.ssb.lds.api.persistence.Persistence;
import org.json.JSONObject;

import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;

/**
 * DataFetcher that gets the data from {@link Persistence}.
 */
public class PersistenceFetcher implements DataFetcher<Map<String, Object>> {

    private final Persistence backend;
    private final String nameSpace;
    private final String entity;

    /**
     * Create a new instance
     *
     * @param backend   the backend to use.
     * @param nameSpace the namespace.
     * @param entity    the entity name.
     */
    public PersistenceFetcher(Persistence backend, String nameSpace, String entity) {
        this.backend = Objects.requireNonNull(backend);
        this.nameSpace = Objects.requireNonNull(nameSpace);
        this.entity = Objects.requireNonNull(entity);
    }

    @Override
    public Map<String, Object> get(DataFetchingEnvironment environment) throws Exception {
        JSONObject entity = backend.read(nameSpace, this.entity, environment.getArgument("id"));
        return entity != null ? entity.toMap() : null;
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
