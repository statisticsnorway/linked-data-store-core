package no.ssb.lds.graphql;

import io.undertow.server.HttpServerExchange;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Deque;
import java.util.Map;
import java.util.Objects;

public class Context {

    public static final String TIMESTAMP_NAME = "timestamp";
    private final HttpServerExchange exchange;
    private ZonedDateTime timestamp;

    public Context(HttpServerExchange exchange) {
        this.exchange = Objects.requireNonNull(exchange);
    }

    public HttpServerExchange getExchange() {
        return this.exchange;
    }

    public synchronized ZonedDateTime getSnapshot() {
        if (this.timestamp == null) {
            Map<String, Deque<String>> parameters = this.exchange.getQueryParameters();
            Deque<String> timestampParameter = parameters.get(TIMESTAMP_NAME);
            String timestamp = timestampParameter.pollFirst();
            if (timestamp == null) {
                this.timestamp = ZonedDateTime.now(ZoneId.of("Etc/UTC"));
            } else {
                this.timestamp = ZonedDateTime.parse(timestamp);
            }
        }
        return this.timestamp;
    }

}
