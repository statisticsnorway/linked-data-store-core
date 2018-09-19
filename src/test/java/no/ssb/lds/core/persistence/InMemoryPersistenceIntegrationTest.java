package no.ssb.lds.core.persistence;

import no.ssb.lds.test.ConfigurationOverride;
import no.ssb.lds.test.ConfigurationProfile;
import no.ssb.lds.test.server.TestServer;
import no.ssb.lds.test.server.TestServerListener;
import org.testng.annotations.Ignore;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import javax.inject.Inject;

@Ignore
@Test
@Listeners(TestServerListener.class)
public class InMemoryPersistenceIntegrationTest extends PersistenceIntegrationTests {

    @Inject
    TestServer server;

    @ConfigurationProfile("mem")
    @ConfigurationOverride({"persistence.provider", "mem"})
    @Test
    public void thatCreateOrOverwriteIsSuccessful() {
        createOrOverwriteTest(server.getPersistence(), server.getSpecification());
    }

    @ConfigurationProfile("mem")
    @ConfigurationOverride({"persistence.provider", "mem"})
    @Test
    public void thatRemoveFriendDoesNotRemoveLinkedTarget() {
        removeFriendAndCreateOrOverwriteTest(server.getPersistence(), server.getSpecification());
    }

    @ConfigurationProfile("mem")
    @ConfigurationOverride({"persistence.provider", "mem"})
    @Test
    public void thatDeleteIsSuccessful() {
        deleteTest(server.getPersistence(), server.getSpecification());
    }

}
