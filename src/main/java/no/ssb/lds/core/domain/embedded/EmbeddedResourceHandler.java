package no.ssb.lds.core.domain.embedded;

import com.fasterxml.jackson.databind.JsonNode;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.HeaderValues;
import io.undertow.util.Headers;
import no.ssb.concurrent.futureselector.SelectableFuture;
import no.ssb.lds.api.persistence.Transaction;
import no.ssb.lds.api.persistence.json.JsonDocument;
import no.ssb.lds.api.persistence.json.JsonTools;
import no.ssb.lds.api.persistence.reactivex.RxJsonPersistence;
import no.ssb.lds.api.specification.Specification;
import no.ssb.lds.core.domain.BodyParser;
import no.ssb.lds.core.domain.resource.ResourceContext;
import no.ssb.lds.core.domain.resource.ResourceElement;
import no.ssb.lds.core.saga.SagaCommands;
import no.ssb.lds.core.saga.SagaExecutionCoordinator;
import no.ssb.lds.core.saga.SagaInput;
import no.ssb.lds.core.saga.SagaRepository;
import no.ssb.lds.core.schema.SchemaRepository;
import no.ssb.lds.core.validation.LinkedDocumentValidationException;
import no.ssb.lds.core.validation.LinkedDocumentValidator;
import no.ssb.saga.api.Saga;
import no.ssb.saga.execution.SagaHandoffResult;
import no.ssb.saga.execution.adapter.AdapterLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.LinkedList;

import static java.util.Optional.ofNullable;
import static no.ssb.lds.api.persistence.json.JsonTools.mapper;

public class EmbeddedResourceHandler implements HttpHandler {

    private static final Logger LOG = LoggerFactory.getLogger(EmbeddedResourceHandler.class);

    private final Specification specification;
    private final SchemaRepository schemaRepository;
    private final ResourceContext resourceContext;
    private final SagaExecutionCoordinator sec;
    private final RxJsonPersistence persistence;
    private final SagaRepository sagaRepository;
    private final BodyParser bodyParser = new BodyParser();

    public EmbeddedResourceHandler(RxJsonPersistence persistence, Specification specification, SchemaRepository schemaRepository, ResourceContext resourceContext, SagaExecutionCoordinator sec, SagaRepository sagaRepository) {
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

        JsonNode jsonNode;
        try (Transaction tx = persistence.createTransaction(true)) {
            JsonDocument jsonDocument = persistence.readDocument(tx, resourceContext.getTimestamp(), resourceContext.getNamespace(), topLevelElement.name(), topLevelElement.id()).blockingGet();
            jsonNode = ofNullable(jsonDocument).map(JsonDocument::jackson).orElse(null);
        }

        if (jsonNode == null) {
            exchange.setStatusCode(404);
            return;
        }

        // TODO consistent API independent of sub-tree json type. i.e. figure out whether we should always wrap
        // TODO result in a json-array?
        JsonNode subTreeRoot = resourceContext.subTree(jsonNode);
        String result;
        if (subTreeRoot == null) {
            result = "[null]";
        } else if (subTreeRoot.isContainerNode()) {
            result = JsonTools.toJson(subTreeRoot);
        } else {
            // wrap simple values in json array.
            result = JsonTools.toJson(mapper.createArrayNode().add(subTreeRoot));
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

                    JsonNode managedDocument;
                    try (Transaction tx = persistence.createTransaction(true)) {
                        JsonDocument jsonDocument = persistence.readDocument(tx, resourceContext.getTimestamp(), namespace, managedDomain, managedDocumentId).blockingGet();
                        managedDocument = ofNullable(jsonDocument).map(JsonDocument::jackson).orElse(null);
                    }

                    if (managedDocument == null) {
                        exchange.setStatusCode(404);
                        return;
                    }

                    String contentType = ofNullable(exchange.getRequestHeaders().get(Headers.CONTENT_TYPE))
                            .map(HeaderValues::getFirst).orElse("application/json");
                    JsonNode embeddedJson = bodyParser.deserializeBody(contentType, message);

                    if (LOG.isTraceEnabled()) {
                        LOG.trace("{} {}\n{}", exchange.getRequestMethod(), exchange.getRequestPath(), message);
                    }

                    mergeJson(resourceContext, managedDocument, embeddedJson);

                    try {
                        LinkedDocumentValidator validator = new LinkedDocumentValidator(specification, schemaRepository);
                        // TODO avoid serialization and de-serialization due to using both jackson and org.json
                        validator.validate(managedDomain, JsonTools.toJson(managedDocument));
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
                    SagaInput sagaInput = new SagaInput(sec.generateTxId(), "PUT", "TODO", namespace, managedDomain, managedDocumentId, resourceContext.getTimestamp(), managedDocument);
                    SelectableFuture<SagaHandoffResult> handoff = sec.handoff(sync, adapterLoader, saga, sagaInput, SagaCommands.getSagaAdminParameterCommands(httpServerExchange));
                    SagaHandoffResult handoffResult = handoff.join();

                    exchange.setStatusCode(200);
                    exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");
                    exchange.getResponseSender().send("{\"saga-execution-id\":\"" + handoffResult.getExecutionId() + "\"}");
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

                    JsonNode rootNode;
                    try (Transaction tx = persistence.createTransaction(true)) {
                        JsonDocument jsonDocument = persistence.readDocument(tx, resourceContext.getTimestamp(), namespace, managedDomain, managedDocumentId).blockingGet();
                        rootNode = ofNullable(jsonDocument).map(JsonDocument::jackson).orElse(null);
                    }

                    if (rootNode == null) {
                        exchange.setStatusCode(404);
                        return;
                    }

                    mergeJson(resourceContext, rootNode, null);

                    boolean sync = exchange.getQueryParameters().getOrDefault("sync", new LinkedList()).stream().anyMatch(s -> "true".equalsIgnoreCase((String) s));

                    Saga saga = sagaRepository.get(SagaRepository.SAGA_CREATE_OR_UPDATE_MANAGED_RESOURCE);

                    AdapterLoader adapterLoader = sagaRepository.getAdapterLoader();
                    SagaInput sagaInput = new SagaInput(sec.generateTxId(), "PUT", "TODO", namespace, managedDomain, managedDocumentId, resourceContext.getTimestamp(), rootNode);
                    SelectableFuture<SagaHandoffResult> handoff = sec.handoff(sync, adapterLoader, saga, sagaInput, SagaCommands.getSagaAdminParameterCommands(httpServerExchange));
                    SagaHandoffResult handoffResult = handoff.join();

                    exchange.setStatusCode(200);
                    exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");
                    exchange.getResponseSender().send("{\"saga-execution-id\":\"" + handoffResult.getExecutionId() + "\"}");
                },
                (exchange1, e) -> {
                    exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain");
                    exchange.getResponseSender().send("Error deleting embedded resource: " + e.getMessage());
                    LOG.warn("", e);
                },
                StandardCharsets.UTF_8);
    }

    public boolean mergeJson(ResourceContext resourceContext, JsonNode documentRootNode, JsonNode subTree) {
        return resourceContext.navigateAndCreateJson(documentRootNode, t -> {
            String embeddedPropertyName = t.resourceElement.name();
            if (t.jsonObject.isArray()) {
                // TODO support array-navigation
                throw new UnsupportedOperationException("array navigation not supported");
            }
            if (subTree == null) {
                t.jsonObject.remove(embeddedPropertyName);
                return true;
            }
            t.jsonObject.set(embeddedPropertyName, subTree);
            return true;
        });
    }


}
