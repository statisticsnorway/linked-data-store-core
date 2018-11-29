package no.ssb.lds.core.controller;

import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import no.ssb.lds.api.persistence.Persistence;
import no.ssb.lds.core.domain.embedded.EmbeddedResourceHandler;
import no.ssb.lds.core.domain.managed.ManagedResourceHandler;
import no.ssb.lds.core.domain.reference.ReferenceResourceHandler;
import no.ssb.lds.core.domain.resource.ResourceContext;
import no.ssb.lds.core.domain.resource.ResourceException;
import no.ssb.lds.core.saga.SagaExecutionCoordinator;
import no.ssb.lds.core.saga.SagaRepository;
import no.ssb.lds.core.schema.SchemaRepository;
import no.ssb.lds.core.specification.Specification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;
import java.time.ZonedDateTime;

class DataController implements HttpHandler {

    private static final Logger LOG = LoggerFactory.getLogger(DataController.class);

    final Specification specification;
    final SchemaRepository schemaRepository;
    final Persistence persistence;
    final SagaExecutionCoordinator sec;
    final SagaRepository sagaRepository;

    DataController(Specification specification, SchemaRepository schemaRepository, Persistence persistence, SagaExecutionCoordinator sec, SagaRepository sagaRepository) {
        this.specification = specification;
        this.schemaRepository = schemaRepository;
        this.persistence = persistence;
        this.sec = sec;
        this.sagaRepository = sagaRepository;
    }

    @Override
    public void handleRequest(HttpServerExchange exchange) throws Exception {
        if (exchange.isInIoThread()) {
            exchange.dispatch(this);
            return;
        }

        ResourceContext resourceContext;
        try {
            ZonedDateTime timestamp = ZonedDateTime.now(ZoneId.of("Etc/UTC"));
            resourceContext = ResourceContext.createResourceContext(specification, exchange.getRequestPath(), timestamp);
        } catch (ResourceException e) {
            exchange.setStatusCode(400);
            exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain");
            exchange.getResponseSender().send(e.getMessage());
            return;
        }

        /*
         * NOTE: Must check reference before embedded, because ref is also an embedded resource.
         */

        if (resourceContext.isReference()) {
            new ReferenceResourceHandler(persistence, specification, resourceContext, sec, sagaRepository).handleRequest(exchange);
            return;
        }

        if (resourceContext.isManaged()) {
            new ManagedResourceHandler(persistence, specification, schemaRepository, resourceContext, sec, sagaRepository).handleRequest(exchange);
            return;
        }

        if (resourceContext.isEmbedded()) {
            new EmbeddedResourceHandler(persistence, specification, schemaRepository, resourceContext, sec, sagaRepository).handleRequest(exchange);
            return;
        }

        exchange.setStatusCode(404);
        exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain");
        exchange.getResponseSender().send("Unsupported resource path: " + exchange.getRequestPath());
    }
}
