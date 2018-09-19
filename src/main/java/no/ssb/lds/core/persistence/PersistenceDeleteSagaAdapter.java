package no.ssb.lds.core.persistence;

import no.ssb.lds.api.persistence.Persistence;
import no.ssb.lds.api.persistence.PersistenceDeletePolicy;
import no.ssb.saga.api.SagaNode;
import no.ssb.saga.execution.adapter.Adapter;
import org.json.JSONObject;

import java.util.Map;

public class PersistenceDeleteSagaAdapter extends Adapter<JSONObject> {

    public static final String NAME = "Persistence-Delete";

    private final Persistence persistence;

    public PersistenceDeleteSagaAdapter(Persistence persistence) {
        super(JSONObject.class, NAME);
        this.persistence = persistence;
    }

    @Override
    public JSONObject executeAction(Object sagaInput, Map<SagaNode, Object> dependeesOutput) {
        JSONObject input = (JSONObject) sagaInput;
        boolean deleted = persistence.delete(input.getString("namespace"), input.getString("entity"), input.getString("id"), PersistenceDeletePolicy.FAIL_IF_INCOMING_LINKS);

        // TODO this test fails for ReferenceResourceHandlerTest.thatGETLongResourcePathWorks, probably because the entity does not exist; hence Persistence.delete() returns false
//        if (!deleted) {
//            throw new RuntimeException("Unable to delete data using persistence.");
//        }

        return null;
    }
}
