package no.ssb.lds.core.persistence;

import no.ssb.config.DynamicConfiguration;
import no.ssb.lds.api.persistence.PersistenceInitializer;
import no.ssb.lds.api.persistence.ProviderName;
import no.ssb.lds.api.persistence.reactivex.RxJsonPersistence;
import no.ssb.lds.api.specification.Specification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class PersistenceConfigurator {

    private static final Logger LOG = LoggerFactory.getLogger(PersistenceConfigurator.class);

    public static RxJsonPersistence configurePersistence(DynamicConfiguration configuration, Specification specification) {
        final String providerId = configuration.evaluateToString("persistence.provider");
        LOG.info("Using persistence provider: {}", providerId);

        ServiceLoader<PersistenceInitializer> loader = ServiceLoader.load(PersistenceInitializer.class);

        List<ServiceLoader.Provider<PersistenceInitializer>> providers = loader.stream()
                .filter(p -> {
                    Class<? extends PersistenceInitializer> type = p.type();
                    ProviderName providerName = type.getDeclaredAnnotation(ProviderName.class);
                    return providerId.equals(providerName.value());
                })
                .collect(Collectors.toList());
        if (providers.isEmpty()) {
            throw new RuntimeException("No persistence providers found for providerId: " + providerId);
        }
        if (providers.size() > 1) {
            throw new RuntimeException("More than one persistence provider found for providerId: " + providerId);
        }

        PersistenceInitializer initializer = providers.get(0).get(); // instantiate persistence-initializer through provider

        if (!providerId.equals(initializer.persistenceProviderId())) {
            throw new RuntimeException("Annotated providerId of persistence-module does not match with the provider-id returned from the initializer instance method");
        }

        Set<String> configurationKeys = initializer.configurationKeys();
        Set<String> missingConfigurationKeys = new LinkedHashSet<>();
        for (String key : configurationKeys) {
            if (configuration.evaluateToString(key) == null) {
                missingConfigurationKeys.add(key);
            }
        }
        if (missingConfigurationKeys.size() > 0) {
            throw new IllegalArgumentException("Configuration missing for: " + missingConfigurationKeys);
        }

        Map<String, String> configurationByKey = new LinkedHashMap<>();
        for (String key : configurationKeys) {
            String value = configuration.evaluateToString(key);
            if (value != null) {
                configurationByKey.put(key, value);
            }
        }

        int maxWaitSeconds = configuration.evaluateToInt("persistence.initialization.max-wait-seconds");
        long start = System.currentTimeMillis();
        for (int i = 1; ; i++) {
            try {
                LOG.info("Persistence provider initialization attempt # {}", i);
                RxJsonPersistence persistence = initializer.initialize(configuration.evaluateToString("namespace.default"), configurationByKey, specification.getManagedDomains());
                LOG.info("Persistence service-provider configured");
                return persistence;
            } catch (RuntimeException e) {
                long durationMs = System.currentTimeMillis() - start;
                long remainingMs = TimeUnit.SECONDS.toMillis(maxWaitSeconds) - durationMs;
                if (remainingMs > 0) {
                    try {
                        Thread.sleep(Math.min(1000, remainingMs));
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Interrupted while attempting to sleep, interrupt status preserved.", e);
                    }
                } else {
                    throw e;
                }
            }
        }
    }
}
