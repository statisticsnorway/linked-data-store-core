package no.ssb.lds.core.controller;

import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import no.ssb.lds.api.persistence.PersistenceException;
import no.ssb.lds.api.persistence.reactivex.RxJsonPersistence;
import no.ssb.lds.api.persistence.Transaction;

import java.net.HttpURLConnection;

class LivenessReadinessController implements HttpHandler {

    final RxJsonPersistence persistence;

    public LivenessReadinessController(RxJsonPersistence persistence) {
        this.persistence = persistence;
    }

    @Override
    public void handleRequest(HttpServerExchange exchange) {

        if (exchange.getRequestPath().startsWith("/health/alive")) {
            exchange.setStatusCode(HttpURLConnection.HTTP_OK);

        } else if (exchange.getRequestPath().startsWith("/health/ready")) {

            try {
                Transaction transaction = persistence.createTransaction(true);
                transaction.cancel();
                exchange.setStatusCode(HttpURLConnection.HTTP_OK);

            } catch (PersistenceException e) {
                exchange.setStatusCode(HttpURLConnection.HTTP_UNAVAILABLE);

            }
        }
    }
}

