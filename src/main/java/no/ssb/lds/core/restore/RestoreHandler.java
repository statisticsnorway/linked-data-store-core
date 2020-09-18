package no.ssb.lds.core.restore;

import de.huxhorn.sulky.ulid.ULID;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import no.ssb.lds.api.persistence.json.JsonTools;
import no.ssb.lds.core.saga.SagaExecutionCoordinator;
import no.ssb.lds.core.txlog.TxlogRawdataPool;

import java.nio.charset.StandardCharsets;
import java.util.Deque;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Optional.ofNullable;

public class RestoreHandler implements HttpHandler {

    private final RestoreContextBySource restoreContextBySource;
    private final TxlogRawdataPool txLogPool;
    private final SagaExecutionCoordinator sec;

    public RestoreHandler(RestoreContextBySource restoreContextBySource, TxlogRawdataPool txLogPool, SagaExecutionCoordinator sec) {
        this.restoreContextBySource = restoreContextBySource;
        this.txLogPool = txLogPool;
        this.sec = sec;
    }

    @Override
    public void handleRequest(HttpServerExchange exchange) throws Exception {
        if (exchange.getRequestMethod().equalToString("get")) {
            new RestoreHandler.GetHandler().handleRequest(exchange);
            return;
        }

        if (exchange.getRequestMethod().equalToString("post")) {
            new RestoreHandler.PostHandler().handleRequest(exchange);
            return;
        }

        exchange.setStatusCode(400);
        exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain");
        exchange.getResponseSender().send("Unsupported restore method: " + exchange.getRequestMethod());
    }

    private class GetHandler implements HttpHandler {
        @Override
        public void handleRequest(HttpServerExchange exchange) throws Exception {
            String source = exchange.getRequestPath().substring("/restore/".length());
            RestoreContext context = restoreContextBySource.map.get(source);
            exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json; charset=utf-8");
            exchange.getResponseSender().send(
                    JsonTools.toJson(context == null ?
                            JsonTools.mapper.createObjectNode() :
                            context.serializeContextState()),
                    StandardCharsets.UTF_8);
        }
    }

    private class PostHandler implements HttpHandler {
        public void handleRequest(HttpServerExchange exchange) throws Exception {
            if (exchange.isInIoThread()) {
                exchange.dispatch(this);
                return;
            }

            ULID.Value fromTxId = ofNullable(exchange.getQueryParameters().get("from"))
                    .map(Deque::peek)
                    .map(ULID::parseULID)
                    .orElse(null);

            boolean fromInclusive = ofNullable(exchange.getQueryParameters().get("fromInclusive"))
                    .map(Deque::peek)
                    .map(Boolean::valueOf)
                    .orElse(Boolean.TRUE);

            ULID.Value toTxId = ofNullable(exchange.getQueryParameters().get("to"))
                    .map(Deque::peek)
                    .map(ULID::parseULID)
                    .orElse(null);

            boolean toInclusive = ofNullable(exchange.getQueryParameters().get("toInclusive"))
                    .map(Deque::peek)
                    .map(Boolean::valueOf)
                    .orElse(Boolean.TRUE);

            String source = exchange.getRequestPath().substring("/restore/".length());
            AtomicBoolean succeeded = new AtomicBoolean();
            RestoreContext initialContext = restoreContextBySource.map.computeIfAbsent(source, s -> {
                succeeded.set(true);
                return new RestoreContext(sec, txLogPool, s, fromTxId, fromInclusive, toTxId, toInclusive)
                        .restore();
            });
            RestoreContext restoreContext;
            if (succeeded.get()) {
                restoreContext = initialContext;
            } else {
                if (initialContext.isDone()) {
                    // attempt to replace existing context with a new one
                    try {
                        restoreContext = restoreContextBySource.map.computeIfPresent(source, (s, oldCtx) -> {
                            if (initialContext == oldCtx) {
                                return new RestoreContext(sec, txLogPool, s, fromTxId, fromInclusive, toTxId, toInclusive)
                                        .restore();
                            } else {
                                throw new ConcurrentRestoreStartException();
                            }
                        });
                    } catch (ConcurrentRestoreStartException ce) {
                        // another client started restore for this source concurrently and we lost the race
                        restoreContext = restoreContextBySource.map.get(source);
                    }
                } else {
                    restoreContext = initialContext;
                }
            }

            exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json; charset=utf-8");
            exchange.getResponseSender().send(JsonTools.toJson(restoreContext.serializeContextState()), StandardCharsets.UTF_8);
        }
    }
}