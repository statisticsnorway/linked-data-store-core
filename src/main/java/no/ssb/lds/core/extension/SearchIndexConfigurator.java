package no.ssb.lds.core.extension;

import no.ssb.config.DynamicConfiguration;
import no.ssb.lds.api.specification.Specification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ServiceLoader;

public class SearchIndexConfigurator {

    private static final Logger LOG = LoggerFactory.getLogger(SearchIndexConfigurator.class);

    public static SearchIndex configureSearchIndex(DynamicConfiguration configuration, Specification specification) {
        final String providerId = configuration.evaluateToString("search.index.provider");

        ServiceLoader<SearchIndexProvider> loader = ServiceLoader.load(SearchIndexProvider.class);
        for (SearchIndexProvider provider : loader) {
            if (providerId.equals(provider.getProviderId())) {
                LOG.info("Search index provider configured: {}", providerId);
                return provider.getSearchIndex();
            }
        }
        throw new RuntimeException("No search index provider found for providerId: " + providerId);
    }
}
