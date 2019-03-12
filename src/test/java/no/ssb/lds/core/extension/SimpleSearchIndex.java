package no.ssb.lds.core.extension;

import no.ssb.lds.api.persistence.DocumentKey;
import no.ssb.lds.api.persistence.flattened.FlattenedDocument;
import no.ssb.lds.api.persistence.json.FlattenedDocumentToJson;
import no.ssb.lds.api.persistence.json.JsonDocument;
import no.ssb.lds.api.persistence.json.JsonToFlattenedDocument;
import no.ssb.lds.api.specification.Specification;

import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

public class SimpleSearchIndex implements SearchIndex {


    private HashMap<DocumentKey, FlattenedDocument> index = new HashMap<>();

    @Override
    public void createOrOverwrite(JsonDocument document, Specification specification) {
        DocumentKey key = document.key();
        JsonToFlattenedDocument converter = new JsonToFlattenedDocument(key.namespace(), key.entity(), key.id(),
                key.timestamp(), document.jackson(), 8192);
        index.put(document.key(), converter.toDocument());
    }

    @Override
    public void delete(JsonDocument document, Specification specification) {
        index.put(document.key(), null);
    }

    @Override
    public List<JsonDocument> search(String query) {
         return index.entrySet().stream()
                 .filter(entry -> entry.getValue() != null)
                 .filter(entry -> entry.getValue().leafNodesByPath().values().stream()
                         .anyMatch(value -> value.value() instanceof String
                                 && ((String) value.value()).toLowerCase().contains(query.toLowerCase())))
                 .map(entry -> new JsonDocument(entry.getKey(), new FlattenedDocumentToJson(entry.getValue()).toJsonNode()))
                 .collect(Collectors.toList());
    }
}
