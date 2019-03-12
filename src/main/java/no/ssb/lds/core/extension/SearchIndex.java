package no.ssb.lds.core.extension;

import no.ssb.lds.api.persistence.json.JsonDocument;
import no.ssb.lds.api.specification.Specification;

import java.util.List;

public interface SearchIndex {

    void createOrOverwrite(JsonDocument document, Specification specification);
    void delete(JsonDocument document, Specification specification);
    List<JsonDocument> search(String query);
}
