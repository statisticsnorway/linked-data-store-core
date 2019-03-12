package no.ssb.lds.graphql.fetcher;

import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import no.ssb.lds.core.extension.SearchIndex;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.stream.Collectors;

/**
 * DataFetcher that gets the data from {@link SearchIndex}.
 */
public class QueryFetcher implements DataFetcher<List<Map<String, Object>>> {

    private final SearchIndex backend;
    private final String nameSpace;
    private final String entity;

    /**
     * Create a new instance
     *
     * @param backend   the backend to use.
     * @param nameSpace the namespace.
     * @param entity    the entity name.
     */
    public QueryFetcher(SearchIndex backend, String nameSpace, String entity) {
        this.backend = Objects.requireNonNull(backend);
        this.nameSpace = Objects.requireNonNull(nameSpace);
        this.entity = Objects.requireNonNull(entity);
    }

    @Override
    public List<Map<String, Object>> get(DataFetchingEnvironment environment) {
        return search(environment.getArgument("query"));
    }

    private List<Map<String, Object>> search(String query) {
        return backend.search(query).stream().map(result -> {
                Map<String, Object> map = result.toMap();
                map.put("__graphql_internal_document_key", result.key());
                return map;
            }
        ).collect(Collectors.toList());
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", QueryFetcher.class.getSimpleName() + "[", "]")
                .add("backend=" + backend)
                .add("nameSpace='" + nameSpace + "'")
                .add("entity='" + entity + "'")
                .toString();
    }
}
