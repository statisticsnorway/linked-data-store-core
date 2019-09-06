package no.ssb.lds.core.txlog;

import com.fasterxml.jackson.databind.JsonNode;
import no.ssb.rawdata.api.RawdataClient;
import no.ssb.rawdata.api.RawdataProducer;
import no.ssb.saga.api.SagaNode;
import no.ssb.saga.execution.adapter.Adapter;

import java.util.Map;

public class AppendTxLogAdapter extends Adapter<JsonNode> {

    public static final String NAME = "TxLog-put-entry";

    final RawdataClient client;
    final String topic;
    final RawdataProducer producer;

    public AppendTxLogAdapter(RawdataClient client, String topic) {
        super(JsonNode.class, NAME);
        this.client = client;
        this.topic = topic;
        this.producer = client.producer(topic);
    }

    @Override
    public JsonNode executeAction(SagaNode sagaNode, Object sagaInput, Map<SagaNode, Object> dependeesOutput) {
        producer.publishBuilders(TxLogTools.sagaInputToTxEntry(producer, (JsonNode) sagaInput, "PUT"));
        return null;
    }

}
