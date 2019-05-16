package no.ssb.lds.graphql.fetcher;

import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.reactivex.Maybe;
import no.ssb.lds.api.persistence.Transaction;
import no.ssb.lds.api.persistence.json.JsonDocument;
import no.ssb.lds.api.persistence.reactivex.RxJsonPersistence;
import no.ssb.lds.graphql.GraphQLContext;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PersistenceLinksFetcher implements DataFetcher<List<Map<String, Object>>> {

    private final String field;
    private final String target;
    private final RxJsonPersistence persistence;
    private final Pattern pattern;
    private final String namespace;

    public PersistenceLinksFetcher(RxJsonPersistence persistence, String namespace, String field, String target) {
        this.field = Objects.requireNonNull(field);
        this.target = Objects.requireNonNull(target);
        this.persistence = Objects.requireNonNull(persistence);
        this.pattern = Pattern.compile("/" + Objects.requireNonNull(target) + "/(?<id>.*)");
        this.namespace = Objects.requireNonNull(namespace);
    }

    @Override
    public List<Map<String, Object>> get(DataFetchingEnvironment environment) {
        Map<String, Object> source = environment.getSource();
        List<String> links = (List<String>) source.get(field);
        List<Map<String, Object>> results = new ArrayList<>();
        if (links == null) {
            return null;
        }
        for (String link : links) {
            Matcher matcher = pattern.matcher(link);
            if (matcher.matches()) {
                String id = matcher.group("id");
                GraphQLContext context = environment.getContext();
                JsonDocument document = readDocument(id, context.getSnapshot());
                results.add(document != null ? document.toMap() : null);
            } else {
                // TODO: Handle.
            }
        }
        return results;
    }

    private JsonDocument readDocument(String id, ZonedDateTime snapshot) {
        try (Transaction tx = persistence.createTransaction(true)) {
            Maybe<JsonDocument> jsonDocumentMaybe = persistence.readDocument(tx, snapshot, namespace, target, id);
            return jsonDocumentMaybe.blockingGet();
        }
    }
}
