package no.ssb.lds.graphql;

import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import no.ssb.lds.api.persistence.Transaction;
import no.ssb.lds.api.persistence.json.JsonDocument;
import no.ssb.lds.api.persistence.json.JsonPersistence;
import org.json.JSONObject;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PersistenceLinkFetcher implements DataFetcher<Map<String, Object>> {

    private final String field;
    private final String target;
    private final JsonPersistence persistence;
    private final Pattern pattern;
    private final String namespace;

    public PersistenceLinkFetcher(JsonPersistence persistence, String namespace, String field, String target) {
        this.field = field;
        this.target = target;
        this.persistence = persistence;
        this.pattern = Pattern.compile("/" + target + "/(?<id>.*)");
        this.namespace = namespace;
    }

    @Override
    public Map<String, Object> get(DataFetchingEnvironment environment) throws Exception {
        Map<String, Object> source = environment.getSource();
        String link = (String) source.get(field);
        Matcher matcher = pattern.matcher(link);
        if (matcher.matches()) {
            String id = matcher.group("id");
            // TODO get snapshot timestamp from client through data-fetching-environment
            ZonedDateTime snapshot = ZonedDateTime.now(ZoneId.of("Etc/UTC"));
            JSONObject entity = readDocument(id, snapshot);
            return entity.toMap();
        }
        return null;
    }

    private JSONObject readDocument(String id, ZonedDateTime snapshot) {
        try (Transaction tx = persistence.createTransaction(true)) {
            CompletableFuture<JsonDocument> future = persistence.read(tx, snapshot, namespace, target, id);
            return future.join().document();
        }
    }
}
