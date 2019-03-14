package no.ssb.lds.core.notification;

import com.fasterxml.jackson.databind.JsonNode;
import no.ssb.lds.api.specification.Specification;
import no.ssb.saga.api.SagaNode;
import no.ssb.saga.execution.adapter.AbortSagaException;
import no.ssb.saga.execution.adapter.Adapter;
import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Map;

public class KafkaCreateOrOverwriteSagaAdapter extends Adapter<JsonNode> {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaCreateOrOverwriteSagaAdapter.class);

    public static final String NAME = "Kafka-Notification-Create-or-Overwrite";
    private final KafkaProducer producer;
    private final Specification specification;

    public KafkaCreateOrOverwriteSagaAdapter(KafkaProducer producer, Specification specification) {
        super(JsonNode.class, NAME);
        this.producer = producer;
        this.specification = specification;
    }

    @Override
    public JsonNode executeAction(Object sagaInput, Map<SagaNode, Object> dependeesOutput) {
        JsonNode input = (JsonNode) sagaInput;
        try {
            producer.send(new ProducerRecord(input.get("entity").textValue(),
                    input.get("entity").textValue()+ " with id: " + input.get("id").toString() + " is added/updated with version: "
                            + input.get("version").asText()+" !")).get();

        } catch (Throwable t) {
            throw new AbortSagaException("Unable to publish notification using kafka.", t);
        }

        return null;
    }
}
