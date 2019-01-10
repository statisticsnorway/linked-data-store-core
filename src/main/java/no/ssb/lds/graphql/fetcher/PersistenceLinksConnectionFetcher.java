package no.ssb.lds.graphql.fetcher;

import graphql.relay.Connection;
import graphql.relay.SimpleListConnection;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import no.ssb.lds.api.persistence.json.JsonPersistence;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A wrapper around {@link PersistenceLinksFetcher} that support relay style connections.
 *
 *
 */
public class PersistenceLinksConnectionFetcher implements DataFetcher<Connection<Map<String, Object>>> {

    private final PersistenceLinksFetcher delegate;

    /**
     * Create a new instance
     * <p>
     * Equivalent to calling
     * <pre><code>
     *  new new PersistenceLinksConnectionFetcher(
     *      new PersistenceLinksFetcher(persistence, namespace, field, target));
     * </code></pre>
     */
    public PersistenceLinksConnectionFetcher(JsonPersistence persistence, String namespace, String field,
                                             String target) {
        this(new PersistenceLinksFetcher(persistence, namespace, field, target));
    }

    /**
     * Create a new instance wrapping the given persistence links fetcher.
     */
    public PersistenceLinksConnectionFetcher(PersistenceLinksFetcher fetcher) {
        this.delegate = Objects.requireNonNull(fetcher);
    }

    @Override
    public Connection<Map<String, Object>> get(DataFetchingEnvironment environment) throws Exception {

        // TODO: Use real streaming.
        // Not really efficient at all but the persistence API does not provide pagination yet.
        // We could use the search API to implement this in a later stage if we put the id of
        // the source in the target of the one to many relations.

        List<Map<String, Object>> list = delegate.get(environment);
        SimpleListConnection<Map<String, Object>> connection = new SimpleListConnection<>(list);

        return connection.get(environment);
    }
}
