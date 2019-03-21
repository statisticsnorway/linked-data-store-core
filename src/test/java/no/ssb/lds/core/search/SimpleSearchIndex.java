package no.ssb.lds.core.search;

import io.reactivex.Completable;
import io.reactivex.Flowable;
import no.ssb.lds.api.persistence.DocumentKey;
import no.ssb.lds.api.persistence.flattened.FlattenedDocument;
import no.ssb.lds.api.persistence.json.JsonDocument;
import no.ssb.lds.api.persistence.json.JsonToFlattenedDocument;
import no.ssb.lds.api.search.SearchIndex;
import no.ssb.lds.api.search.SearchResult;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class SimpleSearchIndex implements SearchIndex {

    private Map<String, FlattenedDocument> index = Collections.synchronizedMap(new HashMap<>());

    @Override
    public Completable createOrOverwrite(JsonDocument document) {
        return Completable.fromAction(() -> {
            DocumentKey key = document.key();
            FlattenedDocument converted = new JsonToFlattenedDocument(key.namespace(), key.entity(), key.id(),
                    null, document.jackson(), 8192).toDocument();
            index.put(converted.key().id(), converted);
        });
    }

    @Override
    public Completable delete(JsonDocument document) {
        return Completable.fromAction(() -> index.put(document.key().id(), null));
    }

    @Override
    public Flowable<SearchResult> search(String query, Integer from, Integer size) {
         return Flowable.fromIterable(index.entrySet().stream()
                 .filter(entry -> entry.getValue() != null)
                 .filter(entry -> entry.getValue().leafNodesByPath().values().stream()
                         .anyMatch(value -> value.value() instanceof String
                                 && ((String) value.value()).toLowerCase().contains(query.toLowerCase())))
                 .map(entry -> new SearchResult(entry.getValue().key()))
                 .collect(Collectors.toList()));
    }
}
