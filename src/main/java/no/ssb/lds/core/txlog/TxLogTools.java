package no.ssb.lds.core.txlog;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import de.huxhorn.sulky.ulid.ULID;
import no.ssb.rawdata.api.RawdataMessage;
import no.ssb.rawdata.api.RawdataProducer;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;

class TxLogTools {

    static final ObjectMapper mapper = new ObjectMapper();

    static RawdataMessage.Builder sagaInputToTxEntry(RawdataProducer producer, JsonNode input, String method) {
        String txId = input.get("txid").textValue();
        String schema = input.get("schema").textValue();
        String namespace = input.get("namespace").textValue();
        String entity = input.get("entity").textValue();
        String id = input.get("id").textValue();
        String versionStr = input.get("version").textValue();
        ZonedDateTime version = ZonedDateTime.parse(versionStr, DateTimeFormatter.ISO_ZONED_DATE_TIME);

        ObjectNode meta = mapper.createObjectNode();
        meta.put("method", method);
        meta.put("schema", schema);
        meta.put("namespace", namespace);
        meta.put("entity", entity);
        meta.put("id", id);
        meta.put("version", versionStr);

        String uri = String.format("%s/%s/%s", entity, id, Date.from(version.toInstant()).getTime());

        RawdataMessage.Builder builder = producer.builder()
                .ulid(ULID.parseULID(txId))
                .position(uri);
        if (input.has("data")) {
            JsonNode data = input.get("data");
            builder.put("data", toBytes(data));
        }
        return builder.put("meta", toBytes(meta));
    }

    static JsonNode txEntryToSagaInput(RawdataMessage message) {
        JsonNode meta = toJson(message.get("meta"));
        ObjectNode node = mapper.createObjectNode();
        node.put("txid", message.ulid().toString());
        node.put("method", meta.get("method").textValue());
        node.put("schema", meta.get("schema").textValue());
        node.put("namespace", meta.get("namespace").textValue());
        node.put("entity", meta.get("entity").textValue());
        node.put("id", meta.get("id").textValue());
        node.put("version", meta.get("version").textValue());
        if (message.keys().contains("data")) {
            JsonNode data = toJson(message.get("data"));
            node.set("data", data);
        }
        return node;
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
