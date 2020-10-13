package no.ssb.lds.core.domain.batch;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.HeaderMap;
import io.undertow.util.HeaderValues;
import io.undertow.util.Headers;
import io.undertow.util.StatusCodes;
import no.ssb.concurrent.futureselector.SelectableFuture;
import no.ssb.lds.api.persistence.Transaction;
import no.ssb.lds.api.persistence.batch.Batch;
import no.ssb.lds.api.persistence.json.JsonTools;
import no.ssb.lds.api.persistence.reactivex.RxJsonPersistence;
import no.ssb.lds.api.specification.Specification;
import no.ssb.lds.core.domain.BodyParser;
import no.ssb.lds.core.saga.SagaCommands;
import no.ssb.lds.core.saga.SagaExecutionCoordinator;
import no.ssb.lds.core.saga.SagaInput;
import no.ssb.lds.core.saga.SagaRepository;
import no.ssb.lds.core.schema.SchemaRepository;
import no.ssb.saga.api.Saga;
import no.ssb.saga.execution.SagaHandoffResult;
import no.ssb.saga.execution.adapter.AdapterLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static java.util.Optional.ofNullable;

public class BatchOperationHandler implements HttpHandler {

    private static final Logger LOG = LoggerFactory.getLogger(BatchOperationHandler.class);

    private final Specification specification;
    private final SchemaRepository schemaRepository;
    private final RxJsonPersistence persistence;
    private final SagaExecutionCoordinator sec;
    private final SagaRepository sagaRepository;

    public BatchOperationHandler(Specification specification, SchemaRepository schemaRepository, RxJsonPersistence persistence, SagaExecutionCoordinator sec, SagaRepository sagaRepository) {
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

        if (exchange.getRequestMethod().equalToString("put")) {
            put(exchange);
        } else {
            exchange.setStatusCode(400);
            exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain");
            exchange.getResponseSender().send("Unsupported batch-operation method: " + exchange.getRequestMethod());
        }
    }

    private void put(HttpServerExchange exchange) {
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

                    String contentType = ofNullable(exchange.getRequestHeaders().get(Headers.CONTENT_TYPE))
                            .map(HeaderValues::getFirst).orElse("application/json");
                    JsonNode requestData = BodyParser.deserializeBody(contentType, requestBody);

                    if (LOG.isTraceEnabled()) {
                        LOG.trace("{} {}\n{}", exchange.getRequestMethod(), exchange.getRequestPath(), requestBody);
                    }

                    String namespace = exchange.getRequestPath().substring("/batch/".length());
                    if (namespace.contains("/")) {
                        namespace = namespace.substring(0, namespace.indexOf("/"));
                    }

                    Batch batch = resolveBatch(requestData);

                    // True if defined and no false values.
                    Map<String, Deque<String>> parameters = exchange.getQueryParameters();
                    boolean sync = parameters.getOrDefault("sync", new LinkedList<>())
                            .stream().noneMatch("false"::equalsIgnoreCase);

                    boolean noTxLogging = ofNullable(exchange.getQueryParameters().get("notxlog"))
                            .map(Deque::peekFirst)
                            .map(Boolean::valueOf)
                            .orElse(Boolean.FALSE);
                    Saga saga = sagaRepository.get(noTxLogging ?
                            SagaRepository.SAGA_BATCH_NO_TX_LOG :
                            SagaRepository.SAGA_BATCH);

                    String source = ofNullable(exchange.getQueryParameters().get("source")).map(Deque::peekFirst).orElse(null);
                    String sourceId = ofNullable(exchange.getQueryParameters().get("sourceId")).map(Deque::peekFirst).orElse(null);

                    AdapterLoader adapterLoader = sagaRepository.getAdapterLoader();
                    SagaInput sagaInput = new SagaInput(sec.generateTxId(), "PUT", "TODO", namespace, source, sourceId, batch.getBatchNode());
                    SelectableFuture<SagaHandoffResult> handoff = sec.handoff(sync, adapterLoader, saga, sagaInput, SagaCommands.getSagaAdminParameterCommands(exchange));
                    SagaHandoffResult handoffResult = handoff.join();

                    HeaderMap responseHeaders = exchange.getResponseHeaders();
                    if (sync) {
                        exchange.setStatusCode(StatusCodes.OK);
                        exchange.endExchange();
                    } else {
                        exchange.setStatusCode(StatusCodes.ACCEPTED);
                        responseHeaders.put(Headers.CONTENT_TYPE, "application/json");
                        exchange.getResponseSender().send("{\"saga-execution-id\":\"" + handoffResult.getExecutionId() + "\"}");
                    }
                },
                (exchange1, e) -> {
                    exchange.setStatusCode(StatusCodes.INTERNAL_SERVER_ERROR);
                    exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain");
                    exchange.getResponseSender().send("Error: " + e.getMessage());
                    LOG.warn("", e);
                },
                StandardCharsets.UTF_8);
    }

    private Batch resolveBatch(JsonNode requestData) {
        try (Transaction tx = persistence.createTransaction(false)) {
            Batch batch = new Batch(requestData);
            Map<Integer, JsonNode> transformedNodesByGroupIndex = resolveDynamicBatchGroups(tx, batch, "");
            Batch transformedBatch = transformBatch(batch, transformedNodesByGroupIndex);
            return transformedBatch;
        }
    }

    private Map<Integer, JsonNode> resolveDynamicBatchGroups(Transaction tx, Batch batch, String namespace) {
        Map<Integer, JsonNode> transformedNodesByGroupIndex = new LinkedHashMap<>();
        for (int i = 0; i < batch.groups().size(); i++) {
            Batch.Group group = batch.groups().get(i);
            if (Batch.GroupType.DELETE == group.groupType()) {
                if (group.hasMatchCriteria()) {
                    ObjectNode groupNode = JsonTools.mapper.createObjectNode();
                    List<String> ids = persistence.resolveMatchInBatchGroup(tx, (Batch.DeleteGroup) group, namespace, specification)
                            .collect(ArrayList<String>::new, ArrayList::add).blockingGet();
                    groupNode.put("operation", "delete");
                    groupNode.put("type", group.type());
                    ArrayNode entries = groupNode.putArray("entries");
                    for (String id : ids) {
                        ObjectNode entryNode = entries.addObject();
                        entryNode.put("id", id);
                        entryNode.put("timestamp", group.getGroupNode().get("timestamp").textValue());
                    }
                    transformedNodesByGroupIndex.put(i, groupNode);
                }
            }
        }
        return transformedNodesByGroupIndex;
    }

    private Batch transformBatch(Batch batch, Map<Integer, JsonNode> transformedNodesByGroupIndex) {
        if (transformedNodesByGroupIndex.isEmpty()) {
            return batch; // no transformation
        }
        if (batch.getBatchNode().isObject()) {
            JsonNode theOnlyGroupNode = transformedNodesByGroupIndex.get(0);
            return new Batch(theOnlyGroupNode); // the only group has been transformed
        }
        ArrayNode batchNode = JsonTools.mapper.createArrayNode();
        for (int i = 0; i < batch.getBatchNode().size(); i++) {
            JsonNode transformedGroupNode = transformedNodesByGroupIndex.get(i);
            if (transformedGroupNode != null) {
                batchNode.add(transformedGroupNode); // use transformed group
            } else {
                batchNode.add(batch.getBatchNode().get(i)); // use original group
            }
        }
        return new Batch(batchNode); // at-least one of the groups has been transformed
    }
}
