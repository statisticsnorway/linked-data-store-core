package no.ssb.lds.core.txlog;

import com.fasterxml.jackson.databind.JsonNode;
import no.ssb.lds.core.saga.SagaInput;
import no.ssb.rawdata.api.RawdataProducer;
import no.ssb.saga.api.SagaNode;
import no.ssb.saga.execution.adapter.Adapter;

import java.util.Map;

public class DeleteTxLogAdapter extends Adapter<JsonNode> {

    public static final String NAME = "TxLog-delete-entry";

    final TxlogRawdataPool pool;

    public DeleteTxLogAdapter(TxlogRawdataPool pool) {
        super(JsonNode.class, NAME);
        this.pool = pool;
    }

    @Override
    public JsonNode executeAction(SagaNode sagaNode, Object input, Map<SagaNode, Object> dependeesOutput) {
        SagaInput sagaInput = new SagaInput((JsonNode) input);
        RawdataProducer producer = pool.producer(sagaInput.source());
        producer.publishBuilders(TxLogTools.sagaInputToTxEntry(producer.builder(), sagaInput));
        return null;
    }
}
