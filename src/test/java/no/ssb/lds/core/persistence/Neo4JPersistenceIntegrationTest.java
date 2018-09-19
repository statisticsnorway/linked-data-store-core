package no.ssb.lds.core.persistence;

import no.ssb.lds.test.ConfigurationOverride;
import no.ssb.lds.test.ConfigurationProfile;
import no.ssb.lds.test.server.TestServer;
import no.ssb.lds.test.server.TestServerListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import javax.inject.Inject;

// https://neo4j.com/developer/java/#neo4j-java-driver

//@Ignore
@Test(groups = "neo4j")
@Listeners(TestServerListener.class)
public class Neo4JPersistenceIntegrationTest extends PersistenceIntegrationTests {

    private static final Logger LOG = LoggerFactory.getLogger(Neo4JPersistenceIntegrationTest.class);

    @Inject
    TestServer server;

    @ConfigurationProfile("neo4j")
    @ConfigurationOverride({
            "persistence.provider", "neo4j",
            "neo4j.driver.url", "bolt://localhost:7687",
            "neo4j.driver.username", "neo4j",
            "neo4j.driver.password", "PasSW0rd"
    })
    @Test
    public void thatCreateOrOverwriteIsSuccessful() {
        createOrOverwriteTest(server.getPersistence(), server.getSpecification());
    }

    @ConfigurationProfile("neo4j")
    @ConfigurationOverride({"persistence.provider", "neo4j"})
    @Test
    public void thatRemoveFriendDoesNotRemoveLinkedTarget() {
        removeFriendAndCreateOrOverwriteTest(server.getPersistence(), server.getSpecification());
    }

    @ConfigurationProfile("neo4j")
    @ConfigurationOverride({"persistence.provider", "neo4j"})
    @Test
    public void thatDeleteIsSuccessful() {
        deleteTest(server.getPersistence(), server.getSpecification());
    }

}
