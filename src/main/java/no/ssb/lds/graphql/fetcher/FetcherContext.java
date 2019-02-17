package no.ssb.lds.graphql.fetcher;

import no.ssb.lds.api.persistence.json.JsonDocument;

import java.util.Map;

public class FetcherContext {
    private final Map<String, Object> map;
    private final JsonDocument document;

    public FetcherContext(Map<String, Object> map, JsonDocument document) {
        this.map = map;
        this.document = document;
    }

    public Map<String, Object> getMap() {
        return map;
    }

    public JsonDocument getDocument() {
        return document;
    }
}
