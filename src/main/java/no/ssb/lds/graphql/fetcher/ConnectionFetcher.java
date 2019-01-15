package no.ssb.lds.graphql.fetcher;

import graphql.relay.Connection;
import graphql.relay.DefaultConnectionCursor;
import graphql.relay.DefaultEdge;
import graphql.relay.Edge;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import no.ssb.lds.api.persistence.json.JsonDocument;
import no.ssb.lds.graphql.GraphQLContext;

import java.time.ZonedDateTime;
import java.util.Map;

import static java.lang.String.format;

public abstract class ConnectionFetcher<T> implements DataFetcher<Connection<T>> {

    private static final String AFTER_ARG_NAME = "after";
    private static final String BEFORE_ARG_NAME = "before";
    private static final String FIRST_ARG_NAME = "first";
    private static final String LAST_ARG_NAME = "last";

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
        Integer last = environment.getArgument(LAST_ARG_NAME);
        if (last != null && last < 0) {
            throw new IllegalArgumentException(format("The page size must not be negative: 'last'=%s", last));
        }
        return last;
    }

    private static Integer getFirstFrom(DataFetchingEnvironment environment) {
        Integer first = environment.getArgument(FIRST_ARG_NAME);
        if (first != null && first < 0) {
            throw new IllegalArgumentException(format("The page size must not be negative: 'first'=%s", first));
        }
        return first;
    }

    protected static Edge<Map<String, Object>> toEdge(JsonDocument document) {
        return new DefaultEdge<>(
                document.document().toMap(),
                new DefaultConnectionCursor(document.key().id())
        );
    }

    @Override
    public Connection<T> get(DataFetchingEnvironment environment) throws Exception {
        return getConnection(
                getSnapshotFrom(environment),
                getAfterFrom(environment),
                getBeforeFrom(environment),
                getLastFrom(environment),
                getFirstFrom(environment)
        );
    }

    abstract Connection<T> getConnection(ZonedDateTime snapshot, String after, String before, Integer last, Integer first);
}
