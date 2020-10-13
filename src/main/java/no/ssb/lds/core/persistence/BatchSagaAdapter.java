package no.ssb.lds.core.persistence;

import com.fasterxml.jackson.databind.JsonNode;
import no.ssb.lds.api.persistence.Transaction;
import no.ssb.lds.api.persistence.batch.Batch;
import no.ssb.lds.api.persistence.reactivex.RxJsonPersistence;
import no.ssb.lds.api.specification.Specification;
import no.ssb.saga.api.SagaNode;
import no.ssb.saga.execution.adapter.Adapter;

import java.util.Map;

public class BatchSagaAdapter extends Adapter<JsonNode> {

    public static final String NAME = "Batch";

    private final RxJsonPersistence persistence;
    private final Specification specification;

    public BatchSagaAdapter(RxJsonPersistence persistence, Specification specification) {
        super(JsonNode.class, NAME);
        this.persistence = persistence;
        this.specification = specification;
    }

    @Override
    public JsonNode executeAction(SagaNode sagaNode, Object sagaInput, Map<SagaNode, Object> dependeesOutput) {
        JsonNode input = (JsonNode) sagaInput;
        Batch batch = new Batch(input.get("batch"));
        try (Transaction tx = persistence.createTransaction(false)) {
            for (Batch.Group group : batch.groups()) {
                if (Batch.GroupType.DELETE == group.groupType()) {
                    persistence.deleteBatchGroup(
                            tx,
                            (Batch.DeleteGroup) group,
                            input.get("namespace").textValue(),
                            specification
                    ).blockingAwait();
                } else if (Batch.GroupType.PUT == group.groupType()) {
                    persistence.putBatchGroup(
                            tx,
                            (Batch.PutGroup) group,
                            input.get("namespace").textValue(),
                            specification
                    ).blockingAwait();
                }
            }
        }
        return null;
    }
}
