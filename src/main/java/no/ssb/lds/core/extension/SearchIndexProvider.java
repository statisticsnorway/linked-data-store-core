package no.ssb.lds.core.extension;

public interface SearchIndexProvider {

    String getProviderId();

    SearchIndex getSearchIndex();
}
