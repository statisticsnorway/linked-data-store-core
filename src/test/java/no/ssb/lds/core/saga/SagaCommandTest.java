package no.ssb.lds.core.saga;

import no.ssb.lds.test.ConfigurationOverride;
import no.ssb.lds.test.client.TestClient;
import no.ssb.lds.test.server.TestServerListener;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import javax.inject.Inject;

@Listeners(TestServerListener.class)
public class SagaCommandTest {

    @Inject
    private TestClient client;

    @Test
    @ConfigurationOverride({"saga.commands.enabled", "false"})
    public void thatFailCommandDoesNotWorkWhenSagaCommandDisabled() {
        String body = "{\"name\":\"pa-test-name\",\"contacts\":[\"/contact/c1\",\"/contact/c2\"]}";
        client.put(String.format("/data/provisionagreement/m1?%s&%s", "sync=true", "saga=failBefore%20E"), body).expect201Created();
    }

    @Test
    @ConfigurationOverride({"saga.commands.enabled", "true"})
    public void thatFailCommandWorksWhenSagaCommandEnabled() {
        String body = "{\"name\":\"pa-test-name\",\"contacts\":[\"/contact/c1\",\"/contact/c2\"]}";
        client.put(String.format("/data/provisionagreement/m1?%s&%s", "sync=true", "saga=failBefore%20E"), body).expectAnyOf(500);
    }
}
