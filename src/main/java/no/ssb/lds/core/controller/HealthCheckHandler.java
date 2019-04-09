package no.ssb.lds.core.controller;

import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import no.ssb.lds.api.persistence.PersistenceException;
import no.ssb.lds.api.persistence.Transaction;
import no.ssb.lds.api.persistence.reactivex.RxJsonPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.HttpURLConnection;
import java.util.Objects;

/**
 * Handler checks persistence health.
 */
public class HealthCheckHandler implements HttpHandler {

    private static final Logger log = LoggerFactory.getLogger(HealthCheckHandler.class);

    public static final String PING_PATH = "/ping";
    public static final String HEALTH_ALIVE_PATH = "/health/alive";
    public static final String HEALTH_READY_PATH = "/health/ready";
    private final RxJsonPersistence persistence;

    public HealthCheckHandler(RxJsonPersistence persistence) {
        this.persistence = Objects.requireNonNull(persistence);
    }

    @Override
    public void handleRequest(HttpServerExchange exchange) {
        // Dispatch here since we are calling persistence.
        if (exchange.isInIoThread()) {
            exchange.dispatch(this);
            return;
        }

        try {
            // TODO: Extract to health() method in RxJsonPersistence interface.
            // TODO: Check search index, log and event provider.
            Transaction transaction = persistence.createTransaction(true);
            transaction.cancel();
            exchange.setStatusCode(HttpURLConnection.HTTP_OK);
        } catch (PersistenceException e) {
            exchange.setStatusCode(HttpURLConnection.HTTP_UNAVAILABLE);
            if (log.isDebugEnabled()) {
                log.debug("health check failed", e);
            } else {
                log.warn("health check failed: {}", e.getMessage());
            }
        } catch (Exception e) {
            exchange.setStatusCode(HttpURLConnection.HTTP_UNAVAILABLE);
            log.warn("unexpected exception during health check {}", persistence, e);
        }
    }
}

