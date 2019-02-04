package no.ssb.lds.core.domain.managed;

import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import no.ssb.concurrent.futureselector.SelectableFuture;
import no.ssb.lds.api.persistence.Transaction;
import no.ssb.lds.api.persistence.json.JsonDocument;
import no.ssb.lds.api.persistence.reactivex.Range;
import no.ssb.lds.api.persistence.reactivex.RxJsonPersistence;
import no.ssb.lds.api.specification.Specification;
import no.ssb.lds.core.domain.resource.ResourceContext;
import no.ssb.lds.core.domain.resource.ResourceElement;
import no.ssb.lds.core.saga.SagaExecutionCoordinator;
import no.ssb.lds.core.saga.SagaRepository;
import no.ssb.lds.core.schema.SchemaRepository;
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

public class ManagedResourceHandler implements HttpHandler {

    private static final Logger LOG = LoggerFactory.getLogger(ManagedResourceHandler.class);

    private final RxJsonPersistence persistence;
    private final Specification specification;
    private final SchemaRepository schemaRepository;
    private final ResourceContext resourceContext;
    private final SagaExecutionCoordinator sec;
    private final SagaRepository sagaRepository;

    public ManagedResourceHandler(RxJsonPersistence persistence, Specification specification, SchemaRepository schemaRepository, ResourceContext resourceContext, SagaExecutionCoordinator sec, SagaRepository sagaRepository) {
        this.persistence = persistence;
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

        JSONArray output = new JSONArray();
        try (Transaction tx = persistence.createTransaction(true)) {
            if (isManagedList) {
                Iterable<JsonDocument> documents = persistence.readDocuments(tx, resourceContext.getTimestamp(), resourceContext.getNamespace(), topLevelElement.name(), Range.unbounded()).blockingIterable();
                for (JsonDocument jsonDocument : documents) {
                    if (jsonDocument.deleted()) {
                        continue;
                    }
                    output.put(jsonDocument.document());
                }
            } else {
                JsonDocument jsonDocument = persistence.readDocument(tx, resourceContext.getTimestamp(), resourceContext.getNamespace(), topLevelElement.name(), topLevelElement.id()).blockingGet();
                if (jsonDocument != null && !jsonDocument.deleted()) {
                    output.put(jsonDocument.document());
                }
            }
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
