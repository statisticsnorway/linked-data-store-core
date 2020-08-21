package no.ssb.lds.graphqlneo4j;

import graphql.ExecutionInput;
import io.undertow.server.HttpServerExchange;

import java.time.Clock;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.util.Deque;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Implementation of {@link GraphQLNeo4jContext}
 */
public class GraphQLNeo4jUndertowContext implements GraphQLNeo4jContext {

    // Name of the snapshot query variable
    static final String SNAPSHOT_QUERY_NAME = "snapshot";

    // Name of the snapshot graphql variable
    static final String SNAPSHOT_VARIABLE_NAME = "__" + SNAPSHOT_QUERY_NAME;

    private final HttpServerExchange exchange;
    private final ZonedDateTime snapshot;
    private final ExecutionInput executionInput;

    GraphQLNeo4jUndertowContext(HttpServerExchange exchange, ExecutionInput executionInput) {
        this.exchange = Objects.requireNonNull(exchange);
        this.executionInput = executionInput;
        // Init snapshot.
        this.snapshot = getSnapshot(exchange.getQueryParameters(), executionInput.getVariables(), Clock.systemUTC());
    }

    /**
     * Gets the snapshot from server exchange query parameters or variables.
     * <p>
     * If both the queryParameters and variable did not contain snapshot value, the given clock is used.
     * The value found in the queryParameters take precedence over the value found in the variables.
     *
     * @throws IllegalArgumentException if the query parameter SNAPSHOT_QUERY_NAME contains more than one value
     *                                  or if variable SNAPSHOT_VARIABLE_NAME is mapped is not a string.
     */
    static ZonedDateTime getSnapshot(Map<String, Deque<String>> queryParameters, Map<String, Object> variables,
                                     Clock clock)
            throws IllegalArgumentException {
        return getSnapshotFromQuery(queryParameters)
                .or(() -> getSnapshotFromVariables(variables))
                .orElse(ZonedDateTime.now(clock));
    }

    /**
     * /**
     * Parses the one parameter that is mapped with the key SNAPSHOT_QUERY_NAME to a {@link ZonedDateTime}.
     * <p>
     * Is the map is empty or the parameter mapped with the key SNAPSHOT_QUERY_NAME is null then the result is empty.
     *
     * @throws IllegalArgumentException if the value to which the key SNAPSHOT_QUERY_NAME contains more than one value.
     */
    private static Optional<ZonedDateTime> getSnapshotFromQuery(Map<String, Deque<String>> queryParameters)
            throws IllegalArgumentException {
        try {
            if (!queryParameters.containsKey(SNAPSHOT_QUERY_NAME)) {
                return Optional.empty();
            }
            Deque<String> snapshotParameter = queryParameters.get(SNAPSHOT_QUERY_NAME);
            if (snapshotParameter.size() != 1) {
                throw new IllegalArgumentException("more than one " + SNAPSHOT_QUERY_NAME + " value in query");
            }
            String snapshotString = snapshotParameter.pollFirst();
            if (snapshotString == null) {
                return Optional.empty();
            }
            return Optional.of(ZonedDateTime.parse(snapshotString));
        } catch (DateTimeParseException e) {
            return Optional.empty();
        }
    }

    /**
     * Parses the value that is mapped with the key SNAPSHOT_VARIABLE_NAME to a {@link ZonedDateTime}.
     * <p>
     * Is the map or the value mapped with the key SNAPSHOT_VARIABLE_NAME is null then the result is empty.
     *
     * @throws IllegalArgumentException if the value to which the key SNAPSHOT_VARIABLE_NAME is mapped is not a string.
     */
    private static Optional<ZonedDateTime> getSnapshotFromVariables(Map<String, Object> variables)
            throws IllegalArgumentException {
        try {
            if (!variables.containsKey(SNAPSHOT_VARIABLE_NAME)) {
                return Optional.empty();
            }
            Object snapshotObject = variables.get(SNAPSHOT_VARIABLE_NAME);
            if (snapshotObject == null) {
                return Optional.empty();
            }
            if (snapshotObject instanceof CharSequence) {
                return Optional.of(ZonedDateTime.parse((CharSequence) snapshotObject));
            } else {
                throw new IllegalArgumentException("the variable " + SNAPSHOT_VARIABLE_NAME + " must be a string");
            }
        } catch (DateTimeParseException e) {
            return Optional.empty();
        }
    }

    @Override
    public HttpServerExchange getExchange() {
        return this.exchange;
    }

    @Override
    public ZonedDateTime getSnapshot() {
        return this.snapshot;
    }

}
