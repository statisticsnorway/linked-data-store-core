package no.ssb.lds.core.domain.reference;

import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import no.ssb.lds.api.persistence.Persistence;
import no.ssb.lds.core.domain.resource.ResourceContext;
import no.ssb.lds.core.domain.resource.ResourceElement;
import no.ssb.lds.core.saga.SagaExecutionCoordinator;
import no.ssb.lds.core.saga.SagaRepository;
import no.ssb.lds.core.specification.Specification;
import no.ssb.saga.api.Saga;
import no.ssb.saga.execution.adapter.AdapterLoader;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.LinkedList;

public class ReferenceResourceHandler implements HttpHandler {

    private static final Logger LOG = LoggerFactory.getLogger(ReferenceResourceHandler.class);

    final Persistence persistence;
    final Specification specification;
    final ResourceContext resourceContext;
    final SagaExecutionCoordinator sec;
    final SagaRepository sagaRepository;

    public ReferenceResourceHandler(Persistence persistence, Specification specification, ResourceContext resourceContext, SagaExecutionCoordinator sec, SagaRepository sagaRepository) {
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

        JSONObject rootNode = persistence.read(resourceContext.getNamespace(), topLevelElement.name(), topLevelElement.id());

        if (rootNode == null) {
            exchange.setStatusCode(404);
            return;
        }

        boolean referenceToExists = resourceContext.referenceToExists(rootNode);

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
                    JSONObject rootNode = persistence.read(namespace, managedDomain, managedDocumentId);
                    boolean referenceToExists = resourceContext.referenceToExists(rootNode);
                    if (referenceToExists) {
                        exchange.setStatusCode(200);
                    } else {
                        new ReferenceJsonHelper(specification, topLevelElement).createReferenceJson(resourceContext, rootNode);

                        boolean sync = exchange.getQueryParameters().getOrDefault("sync", new LinkedList()).stream().anyMatch(s -> "true".equalsIgnoreCase((String) s));

                        Saga saga = sagaRepository.get(SagaRepository.SAGA_CREATE_OR_UPDATE_MANAGED_RESOURCE);

                        AdapterLoader adapterLoader = sagaRepository.getAdapterLoader();
                        String executionId = sec.handoff(sync, adapterLoader, saga, namespace, managedDomain, managedDocumentId, rootNode);

                        exchange.setStatusCode(200);
                        exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");
                        exchange.getResponseSender().send("{\"saga-execution-id\":\"" + executionId + "\"}");
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

        JSONObject rootNode = persistence.read(namespace, managedDomain, managedDocumentId);
        boolean referenceToExists = resourceContext.referenceToExists(rootNode);
        if (!referenceToExists) {
            exchange.setStatusCode(200);
            return;
        } else {
            new ReferenceJsonHelper(specification, topLevelElement).deleteReferenceJson(resourceContext, rootNode);

            boolean sync = exchange.getQueryParameters().getOrDefault("sync", new LinkedList()).stream().anyMatch(s -> "true".equalsIgnoreCase((String) s));

            Saga saga = sagaRepository.get(SagaRepository.SAGA_CREATE_OR_UPDATE_MANAGED_RESOURCE);

            AdapterLoader adapterLoader = sagaRepository.getAdapterLoader();
            String executionId = sec.handoff(sync, adapterLoader, saga, namespace, managedDomain, managedDocumentId, rootNode);

            exchange.setStatusCode(200);
            exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");
            exchange.getResponseSender().send("{\"saga-execution-id\":\"" + executionId + "\"}");
        }
    }
}
