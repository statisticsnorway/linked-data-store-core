package no.ssb.lds.core.utils;

import no.ssb.service.provider.api.ProviderInitializer;
import no.ssb.service.provider.api.ProviderName;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;

public class LDSProviderConfigurator {

    public static <R, T extends ProviderInitializer<R>> T configure(Map<String, String> configuration, String providerId, Class<T> clazz) {
        LDSProviderConfigurator.class.getModule().addUses(clazz);
        ServiceLoader<T> loader = ServiceLoader.load(clazz);
        List<ServiceLoader.Provider<T>> providers = loader.stream()
                .filter(p -> {
                    Class<? extends T> type = p.type();
                    ProviderName providerName = type.getDeclaredAnnotation(ProviderName.class);
                    return providerId.equals(providerName.value());
                })
                .collect(Collectors.toList());
        if (providers.isEmpty()) {
            throw new RuntimeException("No " + clazz.getSimpleName() + " provider found for providerId: " + providerId);
        }
        if (providers.size() > 1) {
            throw new RuntimeException("More than one " + clazz.getSimpleName() + " provider found for providerId: " + providerId);
        }

        T initializer = providers.get(0).get(); // instantiate initializer through provider

        if (!providerId.equals(initializer.providerId())) {
            throw new RuntimeException("Annotated providerId of " + clazz.getSimpleName() + " module does not match with the provider-id returned from the initializer instance method: " + initializer.providerId());
        }

        Set<String> configurationKeys = initializer.configurationKeys();
        Set<String> missingConfigurationKeys = new LinkedHashSet<>();
        for (String key : configurationKeys) {
            if (configuration.get(key) == null) {
                missingConfigurationKeys.add(key);
            }
        }
        if (missingConfigurationKeys.size() > 0) {
            throw new IllegalArgumentException("Configuration missing for: " + missingConfigurationKeys);
        }

        return initializer;
    }
}