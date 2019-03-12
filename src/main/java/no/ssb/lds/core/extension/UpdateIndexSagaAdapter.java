package no.ssb.lds.core.extension;

import com.fasterxml.jackson.databind.JsonNode;
import no.ssb.lds.api.persistence.DocumentKey;
import no.ssb.lds.api.persistence.json.JsonDocument;
import no.ssb.lds.api.specification.Specification;
import no.ssb.saga.api.SagaNode;
import no.ssb.saga.execution.adapter.Adapter;

import java.util.Map;

public class UpdateIndexSagaAdapter extends Adapter<JsonNode> {

    public static final String NAME = "Persistence-Index-Create-or-Overwrite";

    private final SearchIndex indexer;
    private final Specification specification;

    public UpdateIndexSagaAdapter(SearchIndex indexer, Specification specification) {
        super(JsonNode.class, NAME);
        this.indexer = indexer;
        this.specification = specification;
    }

    @Override
    public JsonNode executeAction(Object sagaInput, Map<SagaNode, Object> dependeesOutput) {
        JsonNode input = (JsonNode) sagaInput;
        // We want to overwrite the index for each version, so we set timestamp to null
        indexer.createOrOverwrite(new JsonDocument(new DocumentKey(input.get("namespace").textValue(),
                input.get("entity").textValue(), input.get("id").textValue(), null), input.get("data")),
                specification);
        return null;
    }
}
