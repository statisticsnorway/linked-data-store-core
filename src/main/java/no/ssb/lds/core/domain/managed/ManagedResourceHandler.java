package no.ssb.lds.core.domain.managed;

import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import no.ssb.concurrent.futureselector.SelectableFuture;
import no.ssb.lds.api.persistence.Transaction;
import no.ssb.lds.api.persistence.buffered.BufferedPersistence;
import no.ssb.lds.api.persistence.buffered.DefaultBufferedPersistence;
import no.ssb.lds.api.persistence.buffered.FlattenedDocument;
import no.ssb.lds.api.persistence.buffered.FlattenedDocumentIterator;
import no.ssb.lds.api.persistence.streaming.Persistence;
import no.ssb.lds.core.buffered.DocumentToJson;
import no.ssb.lds.core.domain.resource.ResourceContext;
import no.ssb.lds.core.domain.resource.ResourceElement;
import no.ssb.lds.core.saga.SagaExecutionCoordinator;
import no.ssb.lds.core.saga.SagaRepository;
import no.ssb.lds.core.schema.SchemaRepository;
import no.ssb.lds.core.specification.Specification;
import no.ssb.lds.core.validation.LinkedDocumentValidationException;
import no.ssb.lds.core.validation.LinkedDocumentValidator;
import no.ssb.saga.api.Saga;
import no.ssb.saga.execution.SagaHandoffResult;
import no.ssb.saga.execution.adapter.AdapterLoader;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.concurrent.CompletableFuture;

public class ManagedResourceHandler implements HttpHandler {

    private static final Logger LOG = LoggerFactory.getLogger(ManagedResourceHandler.class);

    private final BufferedPersistence persistence;
    private final Specification specification;
    private final SchemaRepository schemaRepository;
    private final ResourceContext resourceContext;
    private final SagaExecutionCoordinator sec;
    private final SagaRepository sagaRepository;

    public ManagedResourceHandler(Persistence persistence, Specification specification, SchemaRepository schemaRepository, ResourceContext resourceContext, SagaExecutionCoordinator sec, SagaRepository sagaRepository) {
        this.persistence = new DefaultBufferedPersistence(persistence, 8 * 1024);
        this.specification = specification;
        this.schemaRepository = schemaRepository;
        this.resourceContext = resourceContext;
        this.sec = sec;
        this.sagaRepository = sagaRepository;
    }

    @Override
    public void handleRequest(HttpServerExchange exchange) {
        if (exchange.getRequestMethod().equalToString("get")) {
            getManaged(exchange);
        } else if (exchange.getRequestMethod().equalToString("put")) {
            putManaged(exchange);
        } else if (exchange.getRequestMethod().equalToString("post")) {
            putManaged(exchange);
        } else if (exchange.getRequestMethod().equalToString("delete")) {
            deleteManaged(exchange);
        } else {
            exchange.setStatusCode(400);
            exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain");
            exchange.getResponseSender().send("Unsupported managed resource method: " + exchange.getRequestMethod());
        }
    }

    private void getManaged(HttpServerExchange exchange) {
        ResourceElement topLevelElement = resourceContext.getFirstElement();

        boolean isManagedList = topLevelElement.id() == null;

        if (isManagedList && exchange.getQueryParameters().containsKey("schema")) {
            String jsonSchema = schemaRepository.getJsonSchema().getSchemaJson(resourceContext.getFirstElement().name());
            exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json; charset=utf-8");
            exchange.getResponseSender().send(jsonSchema, StandardCharsets.UTF_8);
            return;
        }

        try (Transaction tx = persistence.createTransaction(true)) {
            CompletableFuture<FlattenedDocumentIterator> future;
            if (isManagedList) {
                future = persistence.findAll(tx, resourceContext.getTimestamp(), resourceContext.getNamespace(), topLevelElement.name(), null, 100);
            } else {
                future = persistence.read(tx, resourceContext.getTimestamp(), resourceContext.getNamespace(), topLevelElement.name(), topLevelElement.id());
            }
            FlattenedDocumentIterator iterator = future.join(); // blocking
            JSONArray output = new JSONArray();
            while (iterator.hasNext()) {
                FlattenedDocument document = iterator.next();
                if (document.isDeleted()) {
                    continue;
                }
                output.put(new DocumentToJson(document).toJSONObject());
            }
            if (output.length() == 0) {
                exchange.setStatusCode(404);
            } else if (output.length() == 1) {
                exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json; charset=utf-8");
                exchange.getResponseSender().send(output.getJSONObject(0).toString(), StandardCharsets.UTF_8);
            } else {
                // (output.length() > 1
                exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json; charset=utf-8");
                exchange.getResponseSender().send(output.toString(), StandardCharsets.UTF_8);
            }
            exchange.endExchange();
        }
    }

    private void putManaged(HttpServerExchange exchange) {
        ResourceElement topLevelElement = resourceContext.getFirstElement();
        String namespace = resourceContext.getNamespace();
        String managedDomain = topLevelElement.name();
        String managedDocumentId = topLevelElement.id();

        exchange.getRequestReceiver().receiveFullString(
                (httpServerExchange, requestBody) -> {
                    // check if we received an emtpy payload
                    if ("".equals(requestBody)) {
                        LOG.error("Received empty payload for: {}", exchange.getRequestPath());
                        exchange.setStatusCode(400);
                        exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain");
                        exchange.getResponseSender().send("Payload was empty!");
                        return;
                    }

                    // deserialize request data
                    JSONObject requestData = new JSONObject(requestBody);

                    if (LOG.isTraceEnabled()) {
                        LOG.trace("{} {}\n{}", exchange.getRequestMethod(), exchange.getRequestPath(), requestData.toString(2));
                    }

                    try {
                        LinkedDocumentValidator validator = new LinkedDocumentValidator(specification, schemaRepository);
                        validator.validate(managedDomain, requestData);
                    } catch (LinkedDocumentValidationException ve) {
                        LOG.debug("Schema validation error: {}", ve.getMessage());
                        exchange.setStatusCode(400);
                        exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain");
                        exchange.getResponseSender().send("Schema validation error: " + ve.getMessage());
                        return;
                    }

                    boolean sync = exchange.getQueryParameters().getOrDefault("sync", new LinkedList()).stream().anyMatch(s -> "true".equalsIgnoreCase((String) s));

                    Saga saga = sagaRepository.get(SagaRepository.SAGA_CREATE_OR_UPDATE_MANAGED_RESOURCE);

                    AdapterLoader adapterLoader = sagaRepository.getAdapterLoader();
                    SelectableFuture<SagaHandoffResult> handoff = sec.handoff(sync, adapterLoader, saga, namespace, managedDomain, managedDocumentId, resourceContext.getTimestamp(), requestData);
                    SagaHandoffResult handoffResult = handoff.join();

                    exchange.setStatusCode(200);
                    exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");
                    exchange.getResponseSender().send("{\"saga-execution-id\":\"" + handoffResult.executionId + "\"}");
                },
                (exchange1, e) -> {
                    exchange.setStatusCode(500);
                    exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain");
                    exchange.getResponseSender().send("Error: " + e.getMessage());
                    LOG.warn("", e);
                },
                StandardCharsets.UTF_8);
    }

    private void deleteManaged(HttpServerExchange exchange) {
        ResourceElement topLevelElement = resourceContext.getFirstElement();
        String managedDomain = topLevelElement.name();

        boolean sync = exchange.getQueryParameters().getOrDefault("sync", new LinkedList()).stream().anyMatch(s -> "true".equalsIgnoreCase((String) s));

        Saga saga = sagaRepository.get(SagaRepository.SAGA_DELETE_MANAGED_RESOURCE);

        AdapterLoader adapterLoader = sagaRepository.getAdapterLoader();
        SelectableFuture<SagaHandoffResult> handoff = sec.handoff(sync, adapterLoader, saga, resourceContext.getNamespace(), managedDomain, topLevelElement.id(), resourceContext.getTimestamp(), null);
        SagaHandoffResult handoffResult = handoff.join();

        exchange.setStatusCode(200);
        exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");
        exchange.getResponseSender().send("{\"saga-execution-id\":\"" + handoffResult.executionId + "\"}");
    }
}
