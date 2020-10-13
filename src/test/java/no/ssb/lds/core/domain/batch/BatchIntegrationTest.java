package no.ssb.lds.core.domain.batch;

import no.ssb.lds.core.utils.FileAndClasspathReaderUtils;
import no.ssb.lds.test.ConfigurationOverride;
import no.ssb.lds.test.client.TestClient;
import no.ssb.lds.test.server.TestServerListener;
import org.testng.annotations.Ignore;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import javax.inject.Inject;
import java.nio.charset.StandardCharsets;

@Listeners(TestServerListener.class)
public class BatchIntegrationTest {

    @Inject
    TestClient client;

    @Test
    @Ignore
    @ConfigurationOverride({
            "graphql.schema", "batch/case1/schema.graphql",
            "persistence.provider", "mem", // neo4j
            "neo4j.driver.url", "bolt://localhost:7687",
            "neo4j.driver.username", "neo4j",
            "neo4j.driver.password", "PasSW0rd",
            "neo4j.cypher.show", "true",
            "sagalog.provider", "no.ssb.sagalog.memory.MemorySagaLogInitializer"
    })
    public void thatPutThenDeleteWorks() {
        String putbody1 = FileAndClasspathReaderUtils.getResourceAsString("batch/case1/batch_put_cats.json", StandardCharsets.UTF_8);
        client.put("/batch/data", putbody1).expect200Ok();
        String putbody2 = FileAndClasspathReaderUtils.getResourceAsString("batch/case1/batch_put_cats_and_dogs.json", StandardCharsets.UTF_8);
        client.put("/batch/data", putbody2).expect200Ok();
        String deletebody3 = FileAndClasspathReaderUtils.getResourceAsString("batch/case1/batch_delete_some_cats.json", StandardCharsets.UTF_8);
        client.put("/batch/data", deletebody3).expect200Ok();
    }
}
