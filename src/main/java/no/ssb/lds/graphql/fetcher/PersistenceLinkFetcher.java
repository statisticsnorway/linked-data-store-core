package no.ssb.lds.graphql.fetcher;

import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import no.ssb.lds.api.persistence.Transaction;
import no.ssb.lds.api.persistence.json.JsonDocument;
import no.ssb.lds.api.persistence.json.JsonPersistence;
import no.ssb.lds.api.persistence.reactivex.RxJsonPersistence;
import no.ssb.lds.graphql.GraphQLContext;

import java.time.ZonedDateTime;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PersistenceLinkFetcher implements DataFetcher<Map<String, Object>> {

    private final String field;
    private final String target;
    private final RxJsonPersistence persistence;
    private final Pattern pattern;
    private final String namespace;

    public PersistenceLinkFetcher(RxJsonPersistence persistence, String namespace, String field, String target) {
        this.field = field;
        this.target = target;
        this.persistence = persistence;
        this.pattern = Pattern.compile("/" + target + "/(?<id>.*)");
        this.namespace = namespace;
    }

    @Override
    public Map<String, Object> get(DataFetchingEnvironment environment) {
        Map<String, Object> source = environment.getSource();
        String link = (String) source.get(field);
        Matcher matcher = pattern.matcher(link);
        if (matcher.matches()) {
            String id = matcher.group("id");
            GraphQLContext context = environment.getContext();
            JsonDocument document = readDocument(id, context.getSnapshot());
            return document != null ? document.document().toMap() : null;
        } else {
            // TODO: Handle.
            return null;
        }

    }

    private JsonDocument readDocument(String id, ZonedDateTime snapshot) {
        try (Transaction tx = persistence.createTransaction(true)) {
            return persistence.readDocument(tx, snapshot, namespace, target, id).blockingGet();
        }
    }
}
