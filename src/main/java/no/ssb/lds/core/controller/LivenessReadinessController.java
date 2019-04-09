package no.ssb.lds.core.controller;

import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import no.ssb.lds.api.persistence.PersistenceException;
import no.ssb.lds.api.persistence.Transaction;
import no.ssb.lds.api.persistence.reactivex.RxJsonPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.HttpURLConnection;

/**
 * Handler that provides liveliness and health check.
 */
class LivenessReadinessController implements HttpHandler {

    public static final String HEALTH_PATH = "/health";
    public static final String HEALTH_ALIVE_PATH = HEALTH_PATH + "/alive";
    public static final String HEALTH_READY_PATH = HEALTH_PATH + "/ready";
    private static final Logger log = LoggerFactory.getLogger(LivenessReadinessController.class);
    private final RxJsonPersistence persistence;

    public LivenessReadinessController(RxJsonPersistence persistence) {
        this.persistence = persistence;
    }

    @Override
    public void handleRequest(HttpServerExchange exchange) {
        if (exchange.getRequestPath().startsWith(HEALTH_ALIVE_PATH)) {
            exchange.setStatusCode(HttpURLConnection.HTTP_OK);
        } else if (exchange.getRequestPath().startsWith(HEALTH_READY_PATH)) {

            // Dispatch here since we are calling persistence.
            if (exchange.isInIoThread()) {
                exchange.dispatch(this);
                return;
            }

            try {
                // TODO: Extract to health() method in RxJsonPersistence interface.
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
}

