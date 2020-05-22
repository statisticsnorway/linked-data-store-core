package no.ssb.lds.core.controller;

import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import io.undertow.util.StatusCodes;
import no.ssb.lds.api.persistence.reactivex.RxJsonPersistence;
import no.ssb.lds.api.specification.Specification;
import no.ssb.lds.core.domain.embedded.EmbeddedResourceHandler;
import no.ssb.lds.core.domain.managed.ManagedResourceHandler;
import no.ssb.lds.core.domain.reference.ReferenceResourceHandler;
import no.ssb.lds.core.domain.resource.ResourceContext;
import no.ssb.lds.core.domain.resource.ResourceException;
import no.ssb.lds.core.saga.SagaExecutionCoordinator;
import no.ssb.lds.core.saga.SagaRepository;
import no.ssb.lds.core.schema.SchemaRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.util.Deque;

import static java.util.Optional.ofNullable;

class DataController implements HttpHandler {

    private static final Logger LOG = LoggerFactory.getLogger(DataController.class);

    final Specification specification;
    final SchemaRepository schemaRepository;
    final RxJsonPersistence persistence;
    final SagaExecutionCoordinator sec;
    final SagaRepository sagaRepository;

    DataController(Specification specification, SchemaRepository schemaRepository, RxJsonPersistence persistence, SagaExecutionCoordinator sec, SagaRepository sagaRepository) {
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
            Deque<String> timestampParams = ofNullable(exchange.getQueryParameters().get("timestamp")).orElseGet(() -> exchange.getQueryParameters().get("t"));
            ZonedDateTime timestamp;
            if (timestampParams == null || timestampParams.isEmpty()) {
                // no timestamp given by client, use time now
                timestamp = ZonedDateTime.now(ZoneId.of("Etc/UTC"));
            } else {
                String timestampParam = timestampParams.getLast();
                try {
                    timestamp = ZonedDateTime.parse(timestampParam); // ISO-8601
                } catch (DateTimeParseException e) {
                    // attempt to url-decode query parameter even though this is already done by web-server
                    try {
                        String decodedTimestampParam = URLDecoder.decode(timestampParam, StandardCharsets.UTF_8);
                        timestamp = ZonedDateTime.parse(decodedTimestampParam); // ISO-8601
                    } catch (IllegalArgumentException | DateTimeParseException e2) {
                        exchange.setStatusCode(StatusCodes.BAD_REQUEST);
                        exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain");
                        exchange.getResponseSender().send("The 'timestamp' query-parameter must follow the ISO-8601 standard. Example of a valid formatted timestamp is '2018-12-06T11:05:31.000+01:00'");
                        return;
                    }
                }
                timestamp.withZoneSameInstant(ZoneId.of("Etc/UTC"));
            }
            resourceContext = ResourceContext.createResourceContext(specification, exchange.getRelativePath(), timestamp);
        } catch (ResourceException e) {
            exchange.setStatusCode(StatusCodes.BAD_REQUEST);
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

        exchange.setStatusCode(StatusCodes.NOT_FOUND);
        exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain");
        exchange.getResponseSender().send("Unsupported resource path: " + exchange.getRequestPath());
    }
}
