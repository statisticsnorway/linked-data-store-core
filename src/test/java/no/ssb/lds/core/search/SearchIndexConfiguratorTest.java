package no.ssb.lds.core.search;

import no.ssb.config.DynamicConfiguration;
import no.ssb.config.StoreBasedDynamicConfiguration;
import no.ssb.lds.api.search.SearchIndex;
import org.testng.Assert;
import org.testng.annotations.Test;

public class SearchIndexConfiguratorTest {

    @Test
    public void thatServiceLoadingWorks() {
        DynamicConfiguration configuration = new StoreBasedDynamicConfiguration.Builder()
                .values("graphql.search.enabled", "true")
                .values("search.index.provider", "testSearchIndex")
                .build();
        SearchIndex searchIndex = SearchIndexConfigurator.configureSearchIndex(configuration);
        Assert.assertNotNull(searchIndex);
    }

    @Test
    public void thatServiceLoadingIsSkipped() {
        DynamicConfiguration configuration = new StoreBasedDynamicConfiguration.Builder()
                .values("graphql.search.enabled", "false")
                .values("search.index.provider", "")
                .build();
        SearchIndex searchIndex = SearchIndexConfigurator.configureSearchIndex(configuration);
        Assert.assertNull(searchIndex);
    }

    @Test(expectedExceptions = RuntimeException.class)
    public void thatServiceLoadingIsFails() {
        DynamicConfiguration configuration = new StoreBasedDynamicConfiguration.Builder()
                .values("graphql.search.enabled", "true")
                .values("search.index.provider", "")
                .build();
        SearchIndexConfigurator.configureSearchIndex(configuration);
    }

}
