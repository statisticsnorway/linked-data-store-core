package no.ssb.lds.core.persistence;

import no.ssb.lds.api.persistence.PersistenceDeletePolicy;
import no.ssb.lds.api.persistence.Transaction;
import no.ssb.lds.api.persistence.buffered.BufferedPersistence;
import no.ssb.lds.api.persistence.buffered.DefaultBufferedPersistence;
import no.ssb.lds.api.persistence.streaming.Persistence;
import no.ssb.saga.api.SagaNode;
import no.ssb.saga.execution.adapter.Adapter;
import org.json.JSONObject;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;

public class PersistenceDeleteSagaAdapter extends Adapter<JSONObject> {

    public static final String NAME = "Persistence-Delete";

    private final BufferedPersistence persistence;

    public PersistenceDeleteSagaAdapter(Persistence persistence) {
        super(JSONObject.class, NAME);
        this.persistence = new DefaultBufferedPersistence(persistence, 8 * 1024);
    }

    @Override
    public JSONObject executeAction(Object sagaInput, Map<SagaNode, Object> dependeesOutput) {
        JSONObject input = (JSONObject) sagaInput;
        String versionStr = input.getString("version");
        ZonedDateTime version = ZonedDateTime.parse(versionStr, DateTimeFormatter.ISO_ZONED_DATE_TIME);
        try (Transaction tx = persistence.createTransaction(false)) {
            persistence.markDeleted(
                    tx,
                    input.getString("namespace"),
                    input.getString("entity"),
                    input.getString("id"),
                    version,
                    PersistenceDeletePolicy.FAIL_IF_INCOMING_LINKS
            ).join();
        }
        return null;
    }
}
