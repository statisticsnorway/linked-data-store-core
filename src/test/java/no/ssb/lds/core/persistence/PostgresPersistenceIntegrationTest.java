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
@Test(groups = "postgres")
@Listeners(TestServerListener.class)
public class PostgresPersistenceIntegrationTest extends PersistenceIntegrationTests {

    @Inject
    TestServer server;

    @ConfigurationProfile("postgres")
    @ConfigurationOverride({
            "persistence.provider", "postgres",
            "postgres.driver.host", "localhost",
            "postgres.driver.port", "5432",
            "postgres.driver.user", "lds",
            "postgres.driver.password", "lds",
            "postgres.driver.database", "lds"
    })
    @Test
    public void thatCreateOrOverwriteIsSuccessful() {
        createOrOverwriteTest(server.getPersistence(), server.getSpecification());
    }

    @ConfigurationProfile("postgres")
    @ConfigurationOverride({
            "persistence.provider", "postgres",
            "postgres.driver.host", "localhost",
            "postgres.driver.port", "5432",
            "postgres.driver.user", "lds",
            "postgres.driver.password", "lds",
            "postgres.driver.database", "lds"
    })
    @Test
    public void thatRemoveFriendDoesNotRemoveLinkedTarget() {
        removeFriendAndCreateOrOverwriteTest(server.getPersistence(), server.getSpecification());
    }

    @ConfigurationProfile("postgres")
    @ConfigurationOverride({
            "persistence.provider", "postgres",
            "postgres.driver.host", "localhost",
            "postgres.driver.port", "5432",
            "postgres.driver.user", "lds",
            "postgres.driver.password", "lds",
            "postgres.driver.database", "lds"
    })
    @Test
    public void thatDeleteIsSuccessful() {
        deleteTest(server.getPersistence(), server.getSpecification());
    }

}
