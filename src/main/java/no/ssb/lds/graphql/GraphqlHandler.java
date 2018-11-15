package no.ssb.lds.graphql;

import graphql.ExecutionInput;
import graphql.ExecutionResult;
import graphql.GraphQL;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import io.undertow.util.StatusCodes;
import org.json.JSONObject;

import java.io.IOException;
import java.util.Objects;

/**
 * Handler that executes GraphQL queries.
 */
public class GraphqlHandler implements HttpHandler {

    private final GraphQL graphQl;

    public GraphqlHandler(GraphQL graphQl) {
        this.graphQl = Objects.requireNonNull(graphQl);
    }

    @Override
    public void handleRequest(HttpServerExchange exchange) throws Exception {

        exchange.getRequestReceiver().receiveFullBytes(this::successCallback,
                this::errorCallback
        );
    }

    private void errorCallback(HttpServerExchange exchange, IOException e) {
        e.printStackTrace();
        exchange.setStatusCode(500);
    }

    private void successCallback(HttpServerExchange exchange, byte[] bytes) {

        // Deserialize
        ExecutionInput executionInput = extractInput(bytes);

        // Execute
        ExecutionResult result = graphQl.execute(executionInput);

        // Serialize
        JSONObject json = new JSONObject(result.toSpecification());

        exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");
        exchange.setStatusCode(StatusCodes.OK);
        exchange.getResponseSender().send(json.toString());
    }

    private ExecutionInput extractInput(byte[] data) {

        JSONObject payload = new JSONObject(new String(data));

        return ExecutionInput.newExecutionInput().query(payload.getString("query")).build();
    }
}
