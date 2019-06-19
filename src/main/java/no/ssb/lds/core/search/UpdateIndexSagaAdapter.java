package no.ssb.lds.core.search;

import com.fasterxml.jackson.databind.JsonNode;
import no.ssb.lds.api.persistence.DocumentKey;
import no.ssb.lds.api.persistence.json.JsonDocument;
import no.ssb.lds.api.search.SearchIndex;
import no.ssb.lds.api.specification.Specification;
import no.ssb.saga.api.SagaNode;
import no.ssb.saga.execution.adapter.Adapter;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
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
    public JsonNode executeAction(SagaNode sagaNode, Object sagaInput, Map<SagaNode, Object> dependeesOutput) {
        JsonNode input = (JsonNode) sagaInput;
        String versionStr = input.get("version").textValue();
        ZonedDateTime version = ZonedDateTime.parse(versionStr, DateTimeFormatter.ISO_ZONED_DATE_TIME);
        indexer.createOrOverwrite(new JsonDocument(new DocumentKey(input.get("namespace").textValue(),
                input.get("entity").textValue(), input.get("id").textValue(), version), input.get("data")))
                .blockingAwait();
        return null;
    }
}
