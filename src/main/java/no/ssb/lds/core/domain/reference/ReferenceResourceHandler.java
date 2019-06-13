package no.ssb.lds.core.domain.reference;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import no.ssb.concurrent.futureselector.SelectableFuture;
import no.ssb.lds.api.persistence.Transaction;
import no.ssb.lds.api.persistence.json.JsonDocument;
import no.ssb.lds.api.persistence.reactivex.RxJsonPersistence;
import no.ssb.lds.api.specification.Specification;
import no.ssb.lds.core.domain.resource.ResourceContext;
import no.ssb.lds.core.domain.resource.ResourceElement;
import no.ssb.lds.core.saga.SagaExecutionCoordinator;
import no.ssb.lds.core.saga.SagaRepository;
import no.ssb.saga.api.Saga;
import no.ssb.saga.execution.SagaHandoffResult;
import no.ssb.saga.execution.adapter.AdapterLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.LinkedList;

import static no.ssb.lds.api.persistence.json.JsonTools.mapper;

public class ReferenceResourceHandler implements HttpHandler {

    private static final Logger LOG = LoggerFactory.getLogger(ReferenceResourceHandler.class);

    final RxJsonPersistence persistence;
    final Specification specification;
    final ResourceContext resourceContext;
    final SagaExecutionCoordinator sec;
    final SagaRepository sagaRepository;

    public ReferenceResourceHandler(RxJsonPersistence persistence, Specification specification, ResourceContext resourceContext, SagaExecutionCoordinator sec, SagaRepository sagaRepository) {
        this.persistence = persistence;
        this.specification = specification;
        this.resourceContext = resourceContext;
        this.sec = sec;
        this.sagaRepository = sagaRepository;
    }

    @Override
    public void handleRequest(HttpServerExchange exchange) {
        if (exchange.getRequestMethod().equalToString("get")) {
            getReferenceTo(exchange);
        } else if (exchange.getRequestMethod().equalToString("put")) {
            putReferenceTo(exchange);
        } else if (exchange.getRequestMethod().equalToString("post")) {
            putReferenceTo(exchange);
        } else if (exchange.getRequestMethod().equalToString("delete")) {
            deleteReferenceTo(exchange);
        } else {
            exchange.setStatusCode(400);
            exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain");
            exchange.getResponseSender().send("Unsupported reference resource method: " + exchange.getRequestMethod());
        }
    }

    private void getReferenceTo(HttpServerExchange exchange) {
        ResourceElement topLevelElement = resourceContext.getFirstElement();

        JsonNode jsonNode;
        try (Transaction tx = persistence.createTransaction(true)) {
            JsonDocument jsonDocument = persistence.readDocument(tx, resourceContext.getTimestamp(), resourceContext.getNamespace(), topLevelElement.name(), topLevelElement.id()).blockingGet();
            if (jsonDocument == null || jsonDocument.deleted()) {
                exchange.setStatusCode(404);
                return;
            }
            jsonNode = jsonDocument.jackson();
        }

        boolean referenceToExists = resourceContext.referenceToExists(jsonNode);

        if (referenceToExists) {
            exchange.setStatusCode(200);
        } else {
            exchange.setStatusCode(404);
        }
    }

    private void putReferenceTo(HttpServerExchange exchange) {
        ResourceElement topLevelElement = resourceContext.getFirstElement();
        String namespace = resourceContext.getNamespace();
        String managedDomain = topLevelElement.name();
        String managedDocumentId = topLevelElement.id();

        exchange.getRequestReceiver().receiveFullString(
                (httpServerExchange, message) -> {
                    JsonDocument jsonDocument;
                    try (Transaction tx = persistence.createTransaction(true)) {
                        jsonDocument = persistence.readDocument(tx, resourceContext.getTimestamp(), namespace, managedDomain, managedDocumentId).blockingGet();
                    }
                    boolean referenceToExists = false;
                    if (jsonDocument != null && !jsonDocument.deleted()) {
                        referenceToExists = resourceContext.referenceToExists(jsonDocument.jackson());
                    }
                    if (referenceToExists) {
                        exchange.setStatusCode(200);
                    } else {
                        new ReferenceJsonHelper(specification, topLevelElement).createReferenceJson(resourceContext, jsonDocument.jackson());

                        boolean sync = exchange.getQueryParameters().getOrDefault("sync", new LinkedList<>()).stream().anyMatch(s -> "true".equalsIgnoreCase(s));

                        Saga saga = sagaRepository.get(SagaRepository.SAGA_CREATE_OR_UPDATE_MANAGED_RESOURCE);

                        AdapterLoader adapterLoader = sagaRepository.getAdapterLoader();
                        SelectableFuture<SagaHandoffResult> handoff = sec.handoff(sync, adapterLoader, saga, namespace, managedDomain, managedDocumentId, resourceContext.getTimestamp(), jsonDocument.jackson());
                        SagaHandoffResult sagaHandoffResult = handoff.join();

                        exchange.setStatusCode(200);
                        exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");
                        exchange.getResponseSender().send("{\"saga-execution-id\":\"" + sagaHandoffResult.getExecutionId() + "\"}");
                    }
                },
                (exchange1, e) -> {
                    exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain");
                    exchange.getResponseSender().send("Error: " + e.getMessage());
                    LOG.warn("", e);
                },
                StandardCharsets.UTF_8);
    }

    private void deleteReferenceTo(HttpServerExchange exchange) {
        ResourceElement topLevelElement = resourceContext.getFirstElement();
        String namespace = resourceContext.getNamespace();
        String managedDomain = topLevelElement.name();
        String managedDocumentId = topLevelElement.id();

        ArrayNode output = mapper.createArrayNode();
        try (Transaction tx = persistence.createTransaction(true)) {
            JsonDocument jsonDocument = persistence.readDocument(tx, resourceContext.getTimestamp(), namespace, managedDomain, managedDocumentId).blockingGet();
            if (jsonDocument != null && !jsonDocument.deleted()) {
                output.add(jsonDocument.jackson());

            }
        }
        if (output.size() == 0) {
            exchange.setStatusCode(200);
            exchange.endExchange();
            return;
        }
        if (output.size() > 1) {
            throw new IllegalStateException("More than one document version match.");
        }
        // output.length() == 1
        JsonNode rootNode = output.get(0);
        boolean referenceToExists = resourceContext.referenceToExists(rootNode);
        if (!referenceToExists) {
            exchange.setStatusCode(200);
            exchange.endExchange();
            return;
        }

        new ReferenceJsonHelper(specification, resourceContext.getFirstElement()).deleteReferenceJson(resourceContext, rootNode);

        boolean sync = exchange.getQueryParameters().getOrDefault("sync", new LinkedList()).stream().anyMatch(s -> "true".equalsIgnoreCase((String) s));

        Saga saga = sagaRepository.get(SagaRepository.SAGA_CREATE_OR_UPDATE_MANAGED_RESOURCE);

        AdapterLoader adapterLoader = sagaRepository.getAdapterLoader();
        SelectableFuture<SagaHandoffResult> handoff = sec.handoff(sync, adapterLoader, saga, resourceContext.getNamespace(), resourceContext.getFirstElement().name(), resourceContext.getFirstElement().id(), resourceContext.getTimestamp(), rootNode);
        SagaHandoffResult handoffResult = handoff.join();

        if (sync) {
            exchange.setStatusCode(200);
            exchange.endExchange();
        } else {
            exchange.setStatusCode(200);
            exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");
            exchange.getResponseSender().send("{\"saga-execution-id\":\"" + handoffResult.getExecutionId() + "\"}");
            exchange.endExchange();
        }
    }
}
