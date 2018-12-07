package no.ssb.lds.core.domain.reference;

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
import no.ssb.lds.core.specification.Specification;
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

public class ReferenceResourceHandler implements HttpHandler {

    private static final Logger LOG = LoggerFactory.getLogger(ReferenceResourceHandler.class);

    final BufferedPersistence persistence;
    final Specification specification;
    final ResourceContext resourceContext;
    final SagaExecutionCoordinator sec;
    final SagaRepository sagaRepository;

    public ReferenceResourceHandler(Persistence persistence, Specification specification, ResourceContext resourceContext, SagaExecutionCoordinator sec, SagaRepository sagaRepository) {
        this.persistence = new DefaultBufferedPersistence(persistence, 8 * 1024);
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

        JSONObject jsonObject;
        try (Transaction tx = persistence.createTransaction(true)) {
            FlattenedDocumentIterator flattenedDocumentIterator = persistence.read(tx, resourceContext.getTimestamp(), resourceContext.getNamespace(), topLevelElement.name(), topLevelElement.id()).join();
            if (!flattenedDocumentIterator.hasNext()) {
                exchange.setStatusCode(404);
                return;
            }
            FlattenedDocument document = flattenedDocumentIterator.next();
            if (document.isDeleted()) {
                exchange.setStatusCode(404);
                return;
            }
            jsonObject = new DocumentToJson(document).toJSONObject();
        }

        boolean referenceToExists = resourceContext.referenceToExists(jsonObject);

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
                    FlattenedDocument document = null;
                    try (Transaction tx = persistence.createTransaction(true)) {
                        FlattenedDocumentIterator flattenedDocumentIterator = persistence.read(tx, resourceContext.getTimestamp(), namespace, managedDomain, managedDocumentId).join();
                        if (flattenedDocumentIterator.hasNext()) {
                            document = flattenedDocumentIterator.next();
                        }
                    }
                    boolean referenceToExists = false;
                    JSONObject rootNode = null;
                    if (document != null) {
                        rootNode = new DocumentToJson(document).toJSONObject();
                        referenceToExists = resourceContext.referenceToExists(rootNode);
                    }
                    if (referenceToExists) {
                        exchange.setStatusCode(200);
                    } else {
                        new ReferenceJsonHelper(specification, topLevelElement).createReferenceJson(resourceContext, rootNode);

                        boolean sync = exchange.getQueryParameters().getOrDefault("sync", new LinkedList<>()).stream().anyMatch(s -> "true".equalsIgnoreCase(s));

                        Saga saga = sagaRepository.get(SagaRepository.SAGA_CREATE_OR_UPDATE_MANAGED_RESOURCE);

                        AdapterLoader adapterLoader = sagaRepository.getAdapterLoader();
                        SelectableFuture<SagaHandoffResult> handoff = sec.handoff(sync, adapterLoader, saga, namespace, managedDomain, managedDocumentId, resourceContext.getTimestamp(), rootNode);
                        SagaHandoffResult sagaHandoffResult = handoff.join();

                        exchange.setStatusCode(200);
                        exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");
                        exchange.getResponseSender().send("{\"saga-execution-id\":\"" + sagaHandoffResult.executionId + "\"}");
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

        JSONArray output = new JSONArray();
        try (Transaction tx = persistence.createTransaction(true)) {
            CompletableFuture<FlattenedDocumentIterator> future = persistence.read(tx, resourceContext.getTimestamp(), namespace, managedDomain, managedDocumentId);
            FlattenedDocumentIterator iterator = future.join();
            while (iterator.hasNext()) {
                FlattenedDocument document = iterator.next();
                if (document.isDeleted()) {
                    continue;
                }
                output.put(new DocumentToJson(document).toJSONObject());
            }
        }
        if (output.length() == 0) {
            exchange.setStatusCode(200);
            exchange.endExchange();
            return;
        }
        if (output.length() > 1) {
            throw new IllegalStateException("More than one document version match.");
        }
        // output.length() == 1
        JSONObject rootNode = output.getJSONObject(0);
        boolean referenceToExists = resourceContext.referenceToExists(rootNode);
        if (!referenceToExists) {
            exchange.setStatusCode(200);
            return;
        }

        new ReferenceJsonHelper(specification, resourceContext.getFirstElement()).deleteReferenceJson(resourceContext, rootNode);

        boolean sync = exchange.getQueryParameters().getOrDefault("sync", new LinkedList()).stream().anyMatch(s -> "true".equalsIgnoreCase((String) s));

        Saga saga = sagaRepository.get(SagaRepository.SAGA_CREATE_OR_UPDATE_MANAGED_RESOURCE);

        AdapterLoader adapterLoader = sagaRepository.getAdapterLoader();
        SelectableFuture<SagaHandoffResult> handoff = sec.handoff(sync, adapterLoader, saga, resourceContext.getNamespace(), resourceContext.getFirstElement().name(), resourceContext.getFirstElement().id(), resourceContext.getTimestamp(), rootNode);
        SagaHandoffResult handoffResult = handoff.join();

        exchange.setStatusCode(200);
        exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");
        exchange.getResponseSender().send("{\"saga-execution-id\":\"" + handoffResult.executionId + "\"}");

        exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json; charset=utf-8");
        exchange.getResponseSender().send(output.getJSONObject(0).toString(), StandardCharsets.UTF_8);
        exchange.endExchange();
    }
}
