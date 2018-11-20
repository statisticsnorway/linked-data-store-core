package no.ssb.lds.server;

import no.ssb.config.DynamicConfiguration;
import no.ssb.config.StoreBasedDynamicConfiguration;
import no.ssb.lds.core.UndertowApplication;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DevServer {

    private static final Logger LOG = LoggerFactory.getLogger(DevServer.class);

    public static void main(String[] args) {
        long now = System.currentTimeMillis();

        DynamicConfiguration configuration = new StoreBasedDynamicConfiguration.Builder()
                .propertiesResource(UndertowApplication.getDefaultConfigurationResourcePath())
                .values("persistence.provider", "mem",
                        "persistence.mem.wait.min", "0",
                       "persistence.mem.wait.max", "0"
                )
                .environment("LDS_")
                .systemProperties()
                .build();

        UndertowApplication application = UndertowApplication.initializeUndertowApplication(configuration);

        try {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                LOG.warn("ShutdownHook triggered..");
                application.stop();
            }));

            application.start();

            long time = System.currentTimeMillis() - now;
            LOG.info("Server started in {}ms..", time);

            application.enableSagaExecutionAutomaticDeadlockDetectionAndResolution();

            // wait for termination signal
            try {
                Thread.currentThread().join();
            } catch (InterruptedException e) {
            }
        } finally {
            application.stop();
        }
    }
}
