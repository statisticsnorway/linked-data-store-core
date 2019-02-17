package no.ssb.lds.core.persistence;

import com.fasterxml.jackson.databind.JsonNode;
import no.ssb.lds.api.persistence.PersistenceDeletePolicy;
import no.ssb.lds.api.persistence.Transaction;
import no.ssb.lds.api.persistence.reactivex.RxJsonPersistence;
import no.ssb.saga.api.SagaNode;
import no.ssb.saga.execution.adapter.Adapter;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;

public class PersistenceDeleteSagaAdapter extends Adapter<JsonNode> {

    public static final String NAME = "Persistence-Delete";

    private final RxJsonPersistence persistence;

    public PersistenceDeleteSagaAdapter(RxJsonPersistence persistence) {
        super(JsonNode.class, NAME);
        this.persistence = persistence;
    }

    @Override
    public JsonNode executeAction(Object sagaInput, Map<SagaNode, Object> dependeesOutput) {
        JsonNode input = (JsonNode) sagaInput;
        String versionStr = input.get("version").textValue();
        ZonedDateTime version = ZonedDateTime.parse(versionStr, DateTimeFormatter.ISO_ZONED_DATE_TIME);
        try (Transaction tx = persistence.createTransaction(false)) {
            persistence.markDocumentDeleted(
                    tx,
                    input.get("namespace").textValue(),
                    input.get("entity").textValue(),
                    input.get("id").textValue(),
                    version,
                    PersistenceDeletePolicy.FAIL_IF_INCOMING_LINKS
            ).blockingAwait();
        }
        return null;
    }
}
