package no.ssb.lds.core.persistence;

import no.ssb.lds.api.persistence.OutgoingLink;
import no.ssb.lds.api.persistence.Persistence;
import no.ssb.lds.core.linkeddata.LinkedData;
import no.ssb.lds.core.specification.Specification;
import no.ssb.saga.api.SagaNode;
import no.ssb.saga.execution.adapter.AbortSagaException;
import no.ssb.saga.execution.adapter.Adapter;
import org.json.JSONObject;

import java.util.Map;
import java.util.Set;

public class PersistenceCreateOrOverwriteSagaAdapter extends Adapter<JSONObject> {

    public static final String NAME = "Persistence-Create-or-Overwrite";

    private final Persistence persistence;
    private final Specification specification;

    public PersistenceCreateOrOverwriteSagaAdapter(Persistence persistence, Specification specification) {
        super(JSONObject.class, NAME);
        this.persistence = persistence;
        this.specification = specification;
    }

    @Override
    public JSONObject executeAction(Object sagaInput, Map<SagaNode, Object> dependeesOutput) {
        JSONObject input = (JSONObject) sagaInput;
        Set<OutgoingLink> links = new LinkedData(specification, input.getString("namespace"), input.getString("entity"), input.getString("id"), input.getJSONObject("data")).parse();
        boolean created = persistence.createOrOverwrite(input.getString("namespace"), input.getString("entity"), input.getString("id"), input.getJSONObject("data"), links);
        if (!created) {
            throw new AbortSagaException("Unable to write data using persistence.");
        }
        return null;
    }
}
