package no.ssb.lds.core.saga;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import de.huxhorn.sulky.ulid.ULID;
import no.ssb.lds.api.persistence.json.JsonTools;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public class SagaInput {

    final JsonNode node;

    public SagaInput(JsonNode node) {
        this.node = node;
    }

    public SagaInput(ULID.Value txId, String method, String schema, String namespace, String entity, String id, ZonedDateTime version, JsonNode data) {
        ObjectNode node = JsonTools.mapper.createObjectNode();
        node.put("txid", txId.toString());
        node.put("method", method);
        node.put("schema", schema);
        node.put("namespace", namespace);
        node.put("entity", entity);
        node.put("id", id);
        node.put("version", DateTimeFormatter.ISO_ZONED_DATE_TIME.format(version));
        if (data != null) {
            node.set("data", data);
        }
        this.node = node;
    }

    JsonNode asJsonNode() {
        return node;
    }

    public String txId() {
        return node.get("txid").textValue();
    }

    public String method() {
        return node.get("method").textValue();
    }

    public String schema() {
        return node.get("schema").textValue();
    }

    public String namespace() {
        return node.get("namespace").textValue();
    }

    public String entity() {
        return node.get("entity").textValue();
    }

    public String resourceId() {
        return node.get("id").textValue();
    }

    public ZonedDateTime version() {
        String versionStr = node.get("version").textValue();
        return ZonedDateTime.parse(versionStr, DateTimeFormatter.ISO_ZONED_DATE_TIME);
    }

    public String versionAsString() {
        String versionStr = node.get("version").textValue();
        return versionStr;
    }

    public JsonNode data() {
        return node.get("data");
    }

    @Override
    public String toString() {
        return "SagaInput{" +
                "txId='" + txId() + '\'' +
                ", method='" + method() + '\'' +
                ", schema='" + schema() + '\'' +
                ", namespace='" + namespace() + '\'' +
                ", entity='" + entity() + '\'' +
                ", id='" + resourceId() + '\'' +
                ", version='" + versionAsString() + '\'' +
                ", data=" + data() +
                '}';
    }

    public String toPrettyString() {
        return JsonTools.toPrettyJson(node);
    }
}