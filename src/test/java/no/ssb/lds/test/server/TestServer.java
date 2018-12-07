package no.ssb.lds.test.server;

import no.ssb.config.DynamicConfiguration;
import no.ssb.lds.api.persistence.streaming.Persistence;
import no.ssb.lds.core.UndertowApplication;
import no.ssb.lds.core.saga.SagaExecutionCoordinator;
import no.ssb.lds.core.specification.Specification;

import java.net.MalformedURLException;
import java.net.URL;

public class TestServer implements TestUriResolver {

    final DynamicConfiguration configuration;

    final UndertowApplication application;

    private final int testServerServicePort;

    public TestServer(DynamicConfiguration configuration, int testServerServicePort) {
        this.configuration = configuration;
        this.testServerServicePort = testServerServicePort;
        application = UndertowApplication.initializeUndertowApplication(configuration, testServerServicePort);
    }

    public void start() {
        application.start();
    }

    public void stop() {
        application.stop();
    }

    public String getTestServerHost() {
        return application.getHost();
    }

    public int getTestServerServicePort() {
        return testServerServicePort;
    }

    @Override
    public String testURL(String uri) {
        try {
            URL url = new URL("http", application.getHost(), application.getPort(), uri);
            return url.toExternalForm();
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }

    public Persistence getPersistence() {
        return application.getPersistence();
    }

    public DynamicConfiguration getConfiguration() {
        return configuration;
    }

    public Specification getSpecification() {
        return application.getSpecification();
    }

    public SagaExecutionCoordinator getSagaExecutionCoordinator() {
        return application.getSec();
    }
}
