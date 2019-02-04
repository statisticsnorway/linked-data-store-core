package no.ssb.lds.core.persistence;

import no.ssb.lds.api.persistence.DocumentKey;
import no.ssb.lds.api.persistence.Transaction;
import no.ssb.lds.api.persistence.json.JsonDocument;
import no.ssb.lds.api.persistence.reactivex.RxJsonPersistence;
import no.ssb.lds.api.specification.Specification;
import no.ssb.saga.api.SagaNode;
import no.ssb.saga.execution.adapter.AbortSagaException;
import no.ssb.saga.execution.adapter.Adapter;
import org.json.JSONObject;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;

public class PersistenceCreateOrOverwriteSagaAdapter extends Adapter<JSONObject> {

    public static final String NAME = "Persistence-Create-or-Overwrite";

    private final RxJsonPersistence persistence;
    private final Specification specification;

    public PersistenceCreateOrOverwriteSagaAdapter(RxJsonPersistence persistence, Specification specification) {
        super(JSONObject.class, NAME);
        this.persistence = persistence;
        this.specification = specification;
    }

    @Override
    public JSONObject executeAction(Object sagaInput, Map<SagaNode, Object> dependeesOutput) {
        JSONObject input = (JSONObject) sagaInput;
        String versionStr = input.getString("version");
        ZonedDateTime version = ZonedDateTime.parse(versionStr, DateTimeFormatter.ISO_ZONED_DATE_TIME);
        try (Transaction tx = persistence.createTransaction(false)) {
            persistence.createOrOverwrite(tx, new JsonDocument(new DocumentKey(input.getString("namespace"), input.getString("entity"), input.getString("id"), version), input.getJSONObject("data")), specification).blockingAwait();
        } catch (Throwable t) {
            throw new AbortSagaException("Unable to write data using persistence.", t);
        }
        return null;
    }
}
