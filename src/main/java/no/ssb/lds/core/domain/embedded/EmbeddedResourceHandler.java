package no.ssb.lds.core.domain.embedded;

import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import no.ssb.concurrent.futureselector.SelectableFuture;
import no.ssb.lds.api.persistence.Persistence;
import no.ssb.lds.api.persistence.Transaction;
import no.ssb.lds.api.persistence.buffered.BufferedPersistence;
import no.ssb.lds.api.persistence.buffered.DefaultBufferedPersistence;
import no.ssb.lds.api.persistence.buffered.Document;
import no.ssb.lds.api.persistence.buffered.DocumentIterator;
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
import java.util.Set;

public class EmbeddedResourceHandler implements HttpHandler {

    private static final Logger LOG = LoggerFactory.getLogger(EmbeddedResourceHandler.class);

    private final Specification specification;
    private final SchemaRepository schemaRepository;
    private final ResourceContext resourceContext;
    private final SagaExecutionCoordinator sec;
    private final BufferedPersistence persistence;
    private final SagaRepository sagaRepository;

    public EmbeddedResourceHandler(Persistence persistence, Specification specification, SchemaRepository schemaRepository, ResourceContext resourceContext, SagaExecutionCoordinator sec, SagaRepository sagaRepository) {
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
            getEmbedded(exchange);
        } else if (exchange.getRequestMethod().equalToString("put")) {
            putEmbedded(exchange);
        } else if (exchange.getRequestMethod().equalToString("post")) {
            putEmbedded(exchange);
        } else if (exchange.getRequestMethod().equalToString("delete")) {
            deleteEmbedded(exchange);
        } else {
            exchange.setStatusCode(400);
            exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain");
            exchange.getResponseSender().send("Unsupported embedded resource method: " + exchange.getRequestMethod());
        }
    }

    private void getEmbedded(HttpServerExchange exchange) {
        ResourceElement topLevelElement = resourceContext.getFirstElement();

        JSONObject jsonObject;
        try (Transaction tx = persistence.createTransaction(true)) {
            DocumentIterator documentIterator = persistence.read(tx, resourceContext.getTimestamp(), resourceContext.getNamespace(), topLevelElement.name(), topLevelElement.id()).join();
            if (!documentIterator.hasNext()) {
                exchange.setStatusCode(404);
                return;
            }
            Document document = documentIterator.next();
            if (document.isDeleted()) {
                exchange.setStatusCode(404);
                return;
            }
            jsonObject = new DocumentToJson(document).toJSONObject();
        }

        Object subTreeRoot = resourceContext.subTree(jsonObject);
        String result;
        if (subTreeRoot != null &&
                (subTreeRoot instanceof JSONObject
                        || subTreeRoot instanceof JSONArray)) {
            result = subTreeRoot.toString();
        } else {
            // wrap simple values in json array.
            result = new JSONArray().put(subTreeRoot).toString();
        }
        exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json; charset=utf-8");
        exchange.getResponseSender().send(result, StandardCharsets.UTF_8);
    }

    private void putEmbedded(HttpServerExchange exchange) {
        exchange.getRequestReceiver().receiveFullString(
                (httpServerExchange, message) -> {
                    ResourceElement topLevelElement = resourceContext.getFirstElement();
                    String namespace = resourceContext.getNamespace();
                    String managedDomain = topLevelElement.name();
                    String managedDocumentId = topLevelElement.id();

                    JSONObject managedDocument;
                    try (Transaction tx = persistence.createTransaction(true)) {
                        DocumentIterator documentIterator = persistence.read(tx, resourceContext.getTimestamp(), namespace, managedDomain, managedDocumentId).join();
                        if (!documentIterator.hasNext()) {
                            exchange.setStatusCode(404);
                            return;
                        }
                        Document document = documentIterator.next();
                        managedDocument = new DocumentToJson(document).toJSONObject();
                    }

                    if (LOG.isTraceEnabled()) {
                        LOG.trace("{} {}\n{}", exchange.getRequestMethod(), exchange.getRequestPath(), managedDocument.toString(2));
                    }

                    createEmbeddedJson(resourceContext, managedDocument, message);

                    try {
                        LinkedDocumentValidator validator = new LinkedDocumentValidator(specification, schemaRepository);
                        validator.validate(managedDomain, managedDocument);
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
                    SelectableFuture<SagaHandoffResult> handoff = sec.handoff(sync, adapterLoader, saga, namespace, managedDomain, managedDocumentId, resourceContext.getTimestamp(), managedDocument);
                    SagaHandoffResult handoffResult = handoff.join();

                    exchange.setStatusCode(200);
                    exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");
                    exchange.getResponseSender().send("{\"saga-execution-id\":\"" + handoffResult.executionId + "\"}");
                },
                (exchange1, e) -> {
                    exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain");
                    exchange.getResponseSender().send("Error putting embedded resource: " + e.getMessage());
                    LOG.warn("", e);
                },
                StandardCharsets.UTF_8);
    }

    private void deleteEmbedded(HttpServerExchange exchange) {
        exchange.getRequestReceiver().receiveFullString(
                (httpServerExchange, message) -> {
                    ResourceElement topLevelElement = resourceContext.getFirstElement();
                    String namespace = resourceContext.getNamespace();
                    String managedDomain = topLevelElement.name();
                    String managedDocumentId = topLevelElement.id();

                    JSONObject rootNode;
                    try (Transaction tx = persistence.createTransaction(true)) {
                        DocumentIterator documentIterator = persistence.read(tx, resourceContext.getTimestamp(), namespace, managedDomain, managedDocumentId).join();
                        if (!documentIterator.hasNext()) {
                            exchange.setStatusCode(404);
                            return;
                        }
                        Document document = documentIterator.next();
                        rootNode = new DocumentToJson(document).toJSONObject();
                    }

                    createEmbeddedJson(resourceContext, rootNode, null);

                    boolean sync = exchange.getQueryParameters().getOrDefault("sync", new LinkedList()).stream().anyMatch(s -> "true".equalsIgnoreCase((String) s));

                    Saga saga = sagaRepository.get(SagaRepository.SAGA_CREATE_OR_UPDATE_MANAGED_RESOURCE);

                    AdapterLoader adapterLoader = sagaRepository.getAdapterLoader();
                    SelectableFuture<SagaHandoffResult> handoff = sec.handoff(sync, adapterLoader, saga, namespace, managedDomain, managedDocumentId, resourceContext.getTimestamp(), rootNode);
                    SagaHandoffResult handoffResult = handoff.join();

                    exchange.setStatusCode(200);
                    exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");
                    exchange.getResponseSender().send("{\"saga-execution-id\":\"" + handoffResult.executionId + "\"}");
                },
                (exchange1, e) -> {
                    exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain");
                    exchange.getResponseSender().send("Error deleting embedded resource: " + e.getMessage());
                    LOG.warn("", e);
                },
                StandardCharsets.UTF_8);
    }

    public boolean createEmbeddedJson(ResourceContext resourceContext, JSONObject rootNode, String subTreeJson) {
        return resourceContext.navigateAndCreateJson(rootNode, t -> {
            String embeddedPropertyName = t.resourceElement.name();
            Set<String> jsonTypes = t.resourceElement.getSpecificationElement().getJsonTypes();
            if (jsonTypes.contains("array")) {
                t.jsonObject.put(embeddedPropertyName, subTreeJson == null ? new JSONArray() : new JSONArray(subTreeJson));
                return true;
            } else if (jsonTypes.contains("object")) {
                t.jsonObject.put(embeddedPropertyName, subTreeJson == null ? null : new JSONObject(subTreeJson));
                return true;
            } else if (jsonTypes.contains("string")) {
                t.jsonObject.put(embeddedPropertyName, subTreeJson);
                return true;
            } else {
                throw new IllegalStateException("Unsupported jsonTypes: " + jsonTypes.toString());
            }
        });
    }


}
