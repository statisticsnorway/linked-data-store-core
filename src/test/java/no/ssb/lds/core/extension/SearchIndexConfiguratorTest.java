package no.ssb.lds.core.extension;

import no.ssb.config.DynamicConfiguration;
import no.ssb.config.StoreBasedDynamicConfiguration;
import no.ssb.lds.api.specification.Specification;
import no.ssb.lds.core.specification.JsonSchemaBasedSpecification;
import org.testng.Assert;
import org.testng.annotations.Test;

public class SearchIndexConfiguratorTest {

    @Test
    public void thatServiceLoadingWorks() {
        DynamicConfiguration configuration = new StoreBasedDynamicConfiguration.Builder()
                .values("search.index.provider", "testSearchIndex")
                .build();
        Specification specification = JsonSchemaBasedSpecification.create("spec/schemas/contact.json", "spec/schemas/provisionagreement.json");
        SearchIndex searchIndex = SearchIndexConfigurator.configureSearchIndex(configuration, specification);
        Assert.assertNotNull(searchIndex);
    }

}
