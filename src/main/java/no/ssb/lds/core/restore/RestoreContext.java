package no.ssb.lds.core.restore;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import de.huxhorn.sulky.ulid.ULID;
import no.ssb.concurrent.futureselector.SelectableFuture;
import no.ssb.lds.api.persistence.json.JsonTools;
import no.ssb.lds.core.saga.SagaExecutionCoordinator;
import no.ssb.lds.core.saga.SagaInput;
import no.ssb.lds.core.saga.SagaRepository;
import no.ssb.lds.core.txlog.TxLogTools;
import no.ssb.lds.core.txlog.TxlogRawdataPool;
import no.ssb.rawdata.api.RawdataConsumer;
import no.ssb.rawdata.api.RawdataMessage;
import no.ssb.saga.api.Saga;
import no.ssb.saga.execution.SagaHandoffResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Optional.ofNullable;

public class RestoreContext {

    private static final Logger LOG = LoggerFactory.getLogger(RestoreContext.class);

    private final SagaExecutionCoordinator sec;

    private final TxlogRawdataPool txLogPool;
    private final Thread workerThread;
    private final String source;
    private final ULID.Value fromTxId;
    private final boolean fromInclusive;
    private final ULID.Value toTxId;
    private final boolean toInclusive;
    private final RawdataMessage lastMessage;
    private final AtomicBoolean hasStarted = new AtomicBoolean();
    private final AtomicBoolean stopped = new AtomicBoolean();
    private final AtomicBoolean done = new AtomicBoolean();
    private final AtomicLong messagesRestored = new AtomicLong();
    private final AtomicLong messagesFailed = new AtomicLong();
    private final AtomicLong messagesIgnored = new AtomicLong();

    public RestoreContext(SagaExecutionCoordinator sec, TxlogRawdataPool txLogPool, String source, ULID.Value fromTxId, boolean fromInclusive, ULID.Value toTxId, boolean toInclusive) {
        this.sec = sec;
        this.txLogPool = txLogPool;
        this.source = source;
        this.fromTxId = fromTxId;
        this.fromInclusive = fromInclusive;
        this.toTxId = toTxId;
        this.toInclusive = toInclusive;
        this.workerThread = new Thread(new RestoreWorker());
        this.lastMessage = txLogPool.getLastMessage(source);
    }

    RestoreContext restore() {
        if (hasStarted.compareAndSet(false, true)) {
            workerThread.start();
        }
        return this;
    }

    void stop() {
        stopped.set(true);
        workerThread.interrupt();
    }

    boolean hasStarted() {
        return hasStarted.get();
    }

    boolean isDone() {
        return done.get();
    }

    class RestoreWorker implements Runnable {
        @Override
        public void run() {
            final String topic = txLogPool.topicOf(source);
            try (RawdataConsumer consumer = txLogPool.getClient().consumer(topic, fromTxId, fromInclusive)) {
                while (!stopped.get()) {
                    RawdataMessage message;
                    try {
                        message = consumer.receive(10, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        continue;
                    }
                    if (stopped.get()) {
                        break;
                    }
                    if (message == null) {
                        break;
                    }
                    if (toTxId != null) {
                        int comparison = message.ulid().compareTo(toTxId);
                        if (comparison > 0 || (!toInclusive && (comparison == 0))) {
                            return; // past upper bound set by client
                        }
                    }
                    int comparisonWithLastMessage = message.ulid().compareTo(lastMessage.ulid());
                    if (comparisonWithLastMessage > 0) {
                        return; // past upper bound decided by lastMessage read at start of restore
                    }
                    Saga saga;
                    SagaInput sagaInput = TxLogTools.txEntryToSagaInput(message);
                    if ("DELETE".equalsIgnoreCase(sagaInput.method())) {
                        saga = sec.getSagaRepository().get(SagaRepository.SAGA_DELETE_MANAGED_RESOURCE_NO_TX_LOG);
                    } else if ("PUT".equalsIgnoreCase(sagaInput.method())) {
                        saga = sec.getSagaRepository().get(SagaRepository.SAGA_CREATE_OR_UPDATE_MANAGED_RESOURCE_NO_TX_LOG);
                    } else {
                        LOG.warn("IGNORING message in txlog: ulid={}, method={}, entity={}, id={}, version={}", message.ulid().toString(), sagaInput.method(), sagaInput.entity(), sagaInput.resourceId(), sagaInput.versionAsString());
                        messagesIgnored.incrementAndGet();
                        if (comparisonWithLastMessage == 0) {
                            return; // finished processing lastMessage
                        }
                        continue;
                    }
                    SelectableFuture<SagaHandoffResult> handoff = sec.handoff(true, sec.getSagaRepository().getAdapterLoader(), saga, sagaInput, Collections.emptyMap());
                    SagaHandoffResult handoffResult = handoff.join();
                    if (handoffResult.isSuccess()) {
                        messagesRestored.incrementAndGet();
                    } else {
                        LOG.error(String.format("FAILED to process message: ulid=%s, method=%s, entity=%s, id=%s, version=%s", message.ulid().toString(), sagaInput.method(), sagaInput.entity(), sagaInput.resourceId(), sagaInput.versionAsString()), handoffResult.getFailureCause());
                        messagesFailed.incrementAndGet();
                    }
                    if (comparisonWithLastMessage == 0) {
                        return; // finished processing lastMessage
                    }
                }
            } catch (Exception e) {
                LOG.error("", e);
            } finally {
                done.set(true);
            }
        }
    }

    public JsonNode serializeContextState() {
        ObjectNode ctx = JsonTools.mapper.createObjectNode();
        ctx.put("source", source);
        ctx.put("messagesRestored", messagesRestored.get());
        ctx.put("messagesFailed", messagesFailed.get());
        ctx.put("messagesIgnored", messagesIgnored.get());
        ctx.put("done", done.get());
        ctx.put("fromTxId", ofNullable(fromTxId).map(ULID.Value::toString).orElse(null));
        ctx.put("fromInclusive", fromInclusive);
        ctx.put("toTxId", ofNullable(toTxId).map(ULID.Value::toString).orElse(null));
        ctx.put("toInclusive", toInclusive);
        ctx.put("lastMessageTxId", ofNullable(lastMessage).map(RawdataMessage::ulid).map(ULID.Value::toString).orElse(null));
        ctx.put("hasStarted", hasStarted.get());
        ctx.put("stopped", stopped.get());
        ObjectNode workerThreadObject = ctx.putObject("workerThread");
        workerThreadObject.put("name", workerThread.getName());
        workerThreadObject.put("state", workerThread.getState().name());
        return ctx;
    }
}
