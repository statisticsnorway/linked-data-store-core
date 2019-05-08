package no.ssb.lds.graphql.fetcher;

import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import no.ssb.lds.api.persistence.Transaction;
import no.ssb.lds.api.persistence.json.JsonDocument;
import no.ssb.lds.api.persistence.reactivex.RxJsonPersistence;
import no.ssb.lds.graphql.GraphQLContext;

import java.time.ZonedDateTime;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PersistenceLinkFetcher implements DataFetcher<Map<String, Object>> {

    private final String field;
    private final RxJsonPersistence persistence;
    private final Pattern pattern;
    private final String namespace;

    public PersistenceLinkFetcher(RxJsonPersistence persistence, String namespace, String field, String target) {
        this.field = Objects.requireNonNull(field);
        this.persistence = Objects.requireNonNull(persistence);
        this.pattern = Pattern.compile("/(?<type>" + Objects.requireNonNull(target) + ")/(?<id>.*)");
        this.namespace = Objects.requireNonNull(namespace);
    }

    @Override
    public Map<String, Object> get(DataFetchingEnvironment environment) {
        Map<String, Object> source = environment.getSource();
        String link = (String) source.get(field);
        Matcher matcher = pattern.matcher(link);
        if (matcher.matches()) {
            String id = matcher.group("id");
            String type = matcher.group("type");
            GraphQLContext context = environment.getContext();
            JsonDocument document = readDocument(type, id, context.getSnapshot());
            if (document != null) {
                Map<String, Object> asMap = document.toMap();
                asMap.put("__graphql_internal_document_key", document.key());
                return asMap;
            } else {
                return null;
            }
        } else {
            // TODO: Handle.
            return null;
        }

    }

    private JsonDocument readDocument(String entityName, String id, ZonedDateTime snapshot) {
        try (Transaction tx = persistence.createTransaction(true)) {
            return persistence.readDocument(tx, snapshot, namespace, entityName, id).blockingGet();
        }
    }
}
