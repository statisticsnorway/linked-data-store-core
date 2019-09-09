package no.ssb.lds.core.txlog;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import de.huxhorn.sulky.ulid.ULID;
import no.ssb.lds.core.saga.SagaInput;
import no.ssb.rawdata.api.RawdataMessage;
import no.ssb.rawdata.api.RawdataProducer;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;

class TxLogTools {

    static final ObjectMapper mapper = new ObjectMapper();

    static RawdataMessage.Builder sagaInputToTxEntry(RawdataProducer producer, SagaInput sagaInput) {
        ObjectNode meta = mapper.createObjectNode();
        meta.put("method", sagaInput.method());
        meta.put("schema", sagaInput.schema());
        meta.put("namespace", sagaInput.namespace());
        meta.put("entity", sagaInput.entity());
        meta.put("id", sagaInput.resourceId());
        meta.put("version", sagaInput.versionAsString());

        String uri = String.format("%s/%s/%s", sagaInput.entity(), sagaInput.resourceId(), Date.from(sagaInput.version().toInstant()).getTime());

        RawdataMessage.Builder builder = producer.builder()
                .ulid(ULID.parseULID(sagaInput.txId()))
                .position(uri);
        if (sagaInput.data() != null) {
            builder.put("data", toBytes(sagaInput.data()));
        }
        return builder.put("meta", toBytes(meta));
    }

    static SagaInput txEntryToSagaInput(RawdataMessage message) {
        JsonNode meta = toJson(message.get("meta"));
        JsonNode data = message.keys().contains("data") ? toJson(message.get("data")) : null;
        return new SagaInput(message.ulid(),
                meta.get("method").textValue(),
                meta.get("schema").textValue(),
                meta.get("namespace").textValue(),
                meta.get("entity").textValue(),
                meta.get("id").textValue(),
                ZonedDateTime.parse(meta.get("version").textValue(), DateTimeFormatter.ISO_ZONED_DATE_TIME),
                data);
    }

    private static byte[] toBytes(JsonNode node) {
        try {
            return mapper.writeValueAsBytes(node);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private static JsonNode toJson(byte[] buf) {
        try {
            return mapper.readTree(buf);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
