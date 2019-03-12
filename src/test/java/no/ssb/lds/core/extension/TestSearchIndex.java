package no.ssb.lds.core.extension;

public class TestSearchIndex implements SearchIndexProvider {

    @Override
    public String getProviderId() {
        return "testSearchIndex";
    }

    @Override
    public SearchIndex getSearchIndex() {
        return new SimpleSearchIndex();
    }
}
