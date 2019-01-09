package no.ssb.lds.graphql;

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
 * A context object passed around in the graphql executions.
 */
public class GraphqlContext {

    // Name of the snapshot query variable
    static final String SNAPSHOT_QUERY_NAME = "snapshot";

    // Name of the snapshot graphql variable
    static final String SNAPSHOT_VARIABLE_NAME = "__" + SNAPSHOT_QUERY_NAME;

    // Clock so we can test.
    static Clock CLOCK = Clock.systemUTC();

    private final HttpServerExchange exchange;
    private final ZonedDateTime snapshot;
    private final ExecutionInput executionInput;

    GraphqlContext(HttpServerExchange exchange, ExecutionInput executionInput) {
        this.exchange = Objects.requireNonNull(exchange);
        this.executionInput = executionInput;
        // Init snapshot.
        this.snapshot = getSnapshot(exchange.getQueryParameters(), executionInput.getVariables());
    }

    static ZonedDateTime getSnapshot(Map<String, Deque<String>> queryParameters, Map<String, Object> variables) {
        return getSnapshotFromQuery(queryParameters)
                .or(() -> getSnapshotFromVariables(variables))
                .orElse(ZonedDateTime.now(CLOCK));
    }

    /**
     * Gets the snapshot from server exchange query parameters.
     */
    private static Optional<ZonedDateTime> getSnapshotFromQuery(Map<String, Deque<String>> queryParameters) {
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

    private static Optional<ZonedDateTime> getSnapshotFromVariables(Map<String, Object> variables) {
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

    /**
     * Returns the undertow server exchange that triggered the execution.
     */
    public HttpServerExchange getExchange() {
        return this.exchange;
    }

    public synchronized ZonedDateTime getSnapshot() {
        return this.snapshot;
    }

}
