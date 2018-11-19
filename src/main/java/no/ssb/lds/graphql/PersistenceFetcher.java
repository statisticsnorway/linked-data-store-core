package no.ssb.lds.graphql;

import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import graphql.schema.GraphQLType;
import no.ssb.lds.api.persistence.Persistence;
import org.json.JSONObject;

import java.util.Objects;
import java.util.StringJoiner;

/**
 * DataFetcher that gets the data from {@link Persistence}.
 */
public class PersistenceFetcher implements DataFetcher<JSONObject> {

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
    public JSONObject get(DataFetchingEnvironment environment) throws Exception {
        return backend.read(nameSpace, entity, null);
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
