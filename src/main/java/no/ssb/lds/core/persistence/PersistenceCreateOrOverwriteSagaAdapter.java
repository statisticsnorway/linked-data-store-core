package no.ssb.lds.core.persistence;

import no.ssb.lds.api.persistence.Transaction;
import no.ssb.lds.api.persistence.buffered.BufferedPersistence;
import no.ssb.lds.api.persistence.buffered.DefaultBufferedPersistence;
import no.ssb.lds.api.persistence.buffered.FlattenedDocument;
import no.ssb.lds.api.persistence.streaming.Persistence;
import no.ssb.lds.core.buffered.JsonToDocument;
import no.ssb.lds.core.specification.Specification;
import no.ssb.saga.api.SagaNode;
import no.ssb.saga.execution.adapter.AbortSagaException;
import no.ssb.saga.execution.adapter.Adapter;
import org.json.JSONObject;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;

public class PersistenceCreateOrOverwriteSagaAdapter extends Adapter<JSONObject> {

    public static final String NAME = "Persistence-Create-or-Overwrite";

    private final BufferedPersistence persistence;
    private final Specification specification;

    public PersistenceCreateOrOverwriteSagaAdapter(Persistence persistence, Specification specification) {
        super(JSONObject.class, NAME);
        this.persistence = new DefaultBufferedPersistence(persistence, 8 * 1024);
        this.specification = specification;
    }

    @Override
    public JSONObject executeAction(Object sagaInput, Map<SagaNode, Object> dependeesOutput) {
        JSONObject input = (JSONObject) sagaInput;
        String versionStr = input.getString("version");
        ZonedDateTime version = ZonedDateTime.parse(versionStr, DateTimeFormatter.ISO_ZONED_DATE_TIME);
        FlattenedDocument document = new JsonToDocument(
                input.getString("namespace"),
                input.getString("entity"),
                input.getString("id"),
                version,
                input.getJSONObject("data"),
                8 * 1024
        ).toDocument();
        try (Transaction tx = persistence.createTransaction(false)) {
            persistence.createOrOverwrite(tx, document).join();
        } catch (Throwable t) {
            throw new AbortSagaException("Unable to write data using persistence.", t);
        }
        return null;
    }
}
