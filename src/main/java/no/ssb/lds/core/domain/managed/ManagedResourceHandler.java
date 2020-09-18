package no.ssb.lds.core.domain.managed;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.reactivex.Flowable;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.HeaderMap;
import io.undertow.util.HeaderValues;
import io.undertow.util.Headers;
import io.undertow.util.StatusCodes;
import no.ssb.concurrent.futureselector.SelectableFuture;
import no.ssb.lds.api.persistence.Transaction;
import no.ssb.lds.api.persistence.json.JsonDocument;
import no.ssb.lds.api.persistence.json.JsonTools;
import no.ssb.lds.api.persistence.reactivex.Range;
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
import java.util.Deque;
import java.util.LinkedList;
import java.util.Map;

import static java.util.Optional.ofNullable;
import static no.ssb.lds.api.persistence.json.JsonTools.mapper;

public class ManagedResourceHandler implements HttpHandler {

    private static final Logger LOG = LoggerFactory.getLogger(ManagedResourceHandler.class);

    private final RxJsonPersistence persistence;
    private final Specification specification;
    private final SchemaRepository schemaRepository;
    private final ResourceContext resourceContext;
    private final SagaExecutionCoordinator sec;
    private final SagaRepository sagaRepository;
    private final BodyParser bodyParser = new BodyParser();

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

        exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json; charset=utf-8");

        if (isManagedList && exchange.getQueryParameters().containsKey("schema")) {
            String jsonSchema = schemaRepository.getJsonSchema().getSchemaJson(resourceContext.getFirstElement().name());
            exchange.getResponseSender().send(jsonSchema, StandardCharsets.UTF_8);
            return;
        }

        try (Transaction tx = persistence.createTransaction(true)) {
            if (isManagedList) {
                Iterable<JsonDocument> documents = persistence.readDocuments(tx, resourceContext.getTimestamp(), resourceContext.getNamespace(), topLevelElement.name(), Range.unbounded()).blockingIterable();
                ArrayNode output = mapper.createArrayNode();
                for (JsonDocument jsonDocument : documents) {
                    if (jsonDocument.deleted()) {
                        continue;
                    }
                    output.add(jsonDocument.jackson());
                }
                exchange.getResponseSender().send(JsonTools.toJson(output), StandardCharsets.UTF_8);
            } else {
                if (exchange.getQueryParameters().containsKey("timeline")) {
                    ArrayNode output = mapper.createArrayNode();
                    // TODO Support pagination or time-range based query parameters
                    Flowable<JsonDocument> jsonDocumentFlowable = persistence.readDocumentVersions(tx, resourceContext.getNamespace(), topLevelElement.name(), topLevelElement.id(), Range.unbounded());
                    for (JsonDocument jsonDocument : jsonDocumentFlowable.blockingIterable()) {
                        ObjectNode timeVersionedInstance = output.addObject();
                        timeVersionedInstance.put("version", jsonDocument.key().timestamp().toString());
                        timeVersionedInstance.set("document", jsonDocument.jackson());
                    }
                    if (output.size() == 0) {
                        exchange.setStatusCode(StatusCodes.NOT_FOUND).endExchange();
                        return;
                    }
                    exchange.getResponseSender().send(JsonTools.toJson(output), StandardCharsets.UTF_8);
                } else {
                    JsonDocument jsonDocument = persistence.readDocument(tx, resourceContext.getTimestamp(), resourceContext.getNamespace(), topLevelElement.name(), topLevelElement.id()).blockingGet();
                    if (jsonDocument != null && !jsonDocument.deleted()) {
                        exchange.getResponseSender().send(JsonTools.toJson(jsonDocument.jackson()), StandardCharsets.UTF_8);
                    } else {
                        exchange.setStatusCode(StatusCodes.NOT_FOUND);
                    }
                }
            }
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
                    // check if we received an empty payload
                    if ("".equals(requestBody)) {
                        LOG.error("Received empty payload for: {}", exchange.getRequestPath());
                        exchange.setStatusCode(400);
                        exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain");
                        exchange.getResponseSender().send("Payload was empty!");
                        return;
                    }

                    // check that we have a document id.
                    if (managedDocumentId == null || "".equals(managedDocumentId)) {
                        exchange.setStatusCode(400);
                        exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain");
                        exchange.getResponseSender().send("Id was empty!");
                        return;
                    }

                    String contentType = ofNullable(exchange.getRequestHeaders().get(Headers.CONTENT_TYPE))
                            .map(HeaderValues::getFirst).orElse("application/json");
                    JsonNode requestData = bodyParser.deserializeBody(contentType, requestBody);

                    if (LOG.isTraceEnabled()) {
                        LOG.trace("{} {}\n{}", exchange.getRequestMethod(), exchange.getRequestPath(), requestBody);
                    }

                    try {
                        LinkedDocumentValidator validator = new LinkedDocumentValidator(specification, schemaRepository);
                        validator.validate(managedDomain, requestBody);
                    } catch (LinkedDocumentValidationException ve) {
                        LOG.debug("Schema validation error: {}", ve.getMessage());
                        exchange.setStatusCode(400);
                        exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain");
                        exchange.getResponseSender().send("Schema validation error: " + ve.getMessage());
                        return;
                    }

                    // True if defined and no false values.
                    Map<String, Deque<String>> parameters = exchange.getQueryParameters();
                    boolean sync = parameters.getOrDefault("sync", new LinkedList<>())
                            .stream().noneMatch("false"::equalsIgnoreCase);

                    boolean noTxLogging = ofNullable(exchange.getQueryParameters().get("notxlog"))
                            .map(Deque::peekFirst)
                            .map(Boolean::valueOf)
                            .orElse(Boolean.FALSE);
                    Saga saga = sagaRepository.get(noTxLogging ?
                            SagaRepository.SAGA_CREATE_OR_UPDATE_MANAGED_RESOURCE_NO_TX_LOG :
                            SagaRepository.SAGA_CREATE_OR_UPDATE_MANAGED_RESOURCE);

                    String source = ofNullable(exchange.getQueryParameters().get("source")).map(Deque::peekFirst).orElse(null);
                    String sourceId = ofNullable(exchange.getQueryParameters().get("sourceId")).map(Deque::peekFirst).orElse(null);

                    AdapterLoader adapterLoader = sagaRepository.getAdapterLoader();
                    SagaInput sagaInput = new SagaInput(sec.generateTxId(), "PUT", "TODO", namespace, managedDomain, managedDocumentId, resourceContext.getTimestamp(), source, sourceId, requestData);
                    SelectableFuture<SagaHandoffResult> handoff = sec.handoff(sync, adapterLoader, saga, sagaInput, SagaCommands.getSagaAdminParameterCommands(httpServerExchange));
                    SagaHandoffResult handoffResult = handoff.join();

                    exchange.setStatusCode(StatusCodes.CREATED);
                    exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");
                    exchange.getResponseSender().send("{\"saga-execution-id\":\"" + handoffResult.getExecutionId() + "\"}");
                },
                (exchange1, e) -> {
                    exchange.setStatusCode(StatusCodes.INTERNAL_SERVER_ERROR);
                    exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain");
                    exchange.getResponseSender().send("Error: " + e.getMessage());
                    LOG.warn("", e);
                },
                StandardCharsets.UTF_8);
    }

    private void deleteManaged(HttpServerExchange exchange) {
        ResourceElement topLevelElement = resourceContext.getFirstElement();
        String managedDomain = topLevelElement.name();

        // True if defined and no false values.
        Map<String, Deque<String>> parameters = exchange.getQueryParameters();
        boolean sync = parameters.getOrDefault("sync", new LinkedList<>())
                .stream().noneMatch("false"::equalsIgnoreCase);

        boolean noTxLogging = ofNullable(exchange.getQueryParameters().get("notxlog"))
                .map(Deque::peekFirst)
                .map(Boolean::valueOf)
                .orElse(Boolean.FALSE);
        Saga saga = sagaRepository.get(noTxLogging ?
                SagaRepository.SAGA_DELETE_MANAGED_RESOURCE_NO_TX_LOG :
                SagaRepository.SAGA_DELETE_MANAGED_RESOURCE);

        String source = ofNullable(exchange.getQueryParameters().get("source")).map(Deque::peekFirst).orElse(null);
        String sourceId = ofNullable(exchange.getQueryParameters().get("sourceId")).map(Deque::peekFirst).orElse(null);

        AdapterLoader adapterLoader = sagaRepository.getAdapterLoader();
        SagaInput sagaInput = new SagaInput(sec.generateTxId(), "DELETE", "TODO", resourceContext.getNamespace(), managedDomain, topLevelElement.id(), resourceContext.getTimestamp(), source, sourceId, null);
        SelectableFuture<SagaHandoffResult> handoff = sec.handoff(sync, adapterLoader, saga, sagaInput, SagaCommands.getSagaAdminParameterCommands(exchange));
        SagaHandoffResult handoffResult = handoff.join();

        HeaderMap responseHeaders = exchange.getResponseHeaders();
        if (sync) {
            // Workaround https://bugs.openjdk.java.net/browse/JDK-8211437
            // 204 MUST come with a content-size of 0.
            responseHeaders.add(Headers.CONTENT_LENGTH, 0);
            exchange.setStatusCode(StatusCodes.NO_CONTENT);
            exchange.endExchange();
        } else {
            exchange.setStatusCode(StatusCodes.ACCEPTED);
            responseHeaders.put(Headers.CONTENT_TYPE, "application/json");
            exchange.getResponseSender().send("{\"saga-execution-id\":\"" + handoffResult.getExecutionId() + "\"}");
        }


    }
}
