package no.ssb.lds.core.restore;

import com.fasterxml.jackson.databind.JsonNode;
import de.huxhorn.sulky.ulid.ULID;
import no.ssb.lds.api.persistence.Transaction;
import no.ssb.lds.api.persistence.json.JsonTools;
import no.ssb.lds.api.persistence.reactivex.RxJsonPersistence;
import no.ssb.lds.core.txlog.TxlogRawdataPool;
import no.ssb.lds.test.ConfigurationOverride;
import no.ssb.lds.test.client.TestClient;
import no.ssb.lds.test.server.TestServer;
import no.ssb.lds.test.server.TestServerListener;
import no.ssb.rawdata.api.RawdataClient;
import no.ssb.rawdata.api.RawdataConsumer;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import javax.inject.Inject;
import java.util.concurrent.TimeUnit;

import static no.ssb.lds.core.utils.FileAndClasspathReaderUtils.readFileOrClasspathResource;
import static org.testng.Assert.assertEquals;

@Listeners(TestServerListener.class)
public class RestoreTest {

    @Inject
    private TestClient client;

    @Inject
    private TestServer server;

    @Test
    @ConfigurationOverride({
            "dummy.with.unique.test.value", "restoreTxLogAndVerify",
            "txlog.rawdata.provider", "memory",
            "specification.schema", "spec/demo/contact.json,spec/demo/provisionagreement.json"
    })
    public void restoreTxLogAndVerify() throws Exception {
        produceSevenEntriesAndWipePersistence();
        JsonNode ctx = initiateRestoreAndWaitForCompletion("default", null);

        // Verify
        assertEquals(countMessagesInTxLog(), 7);
        assertEquals(ctx.get("messagesRestored").longValue(), 7);
        client.get("/data/contact/4b2ef").expect200Ok();
        client.get("/data/provisionagreement/2a41c").expect200Ok();
    }

    @Test
    @ConfigurationOverride({
            "dummy.with.unique.test.value", "restoreFromThirdElementInclusive",
            "txlog.rawdata.provider", "memory",
            "specification.schema", "spec/demo/contact.json,spec/demo/provisionagreement.json"
    })
    public void restoreFromThirdElementInclusive() throws Exception {
        produceSevenEntriesAndWipePersistence();
        ULID.Value txIdOfThirdElement = getTxIdOfTheNthElement(3);
        JsonNode ctx = initiateRestoreAndWaitForCompletion("default", "fromInclusive=true&from=" + txIdOfThirdElement.toString());

        // Verify
        assertEquals(countMessagesInTxLog(), 7);
        assertEquals(ctx.get("messagesRestored").longValue(), 5);
        client.get("/data/contact/4b2ef").expect200Ok();
        client.get("/data/contact/821aa").expect404NotFound();
        client.get("/data/provisionagreement/2a41c").expect200Ok();
    }

    @Test
    @ConfigurationOverride({
            "dummy.with.unique.test.value", "restoreFromThirdElementExclusive",
            "txlog.rawdata.provider", "memory",
            "specification.schema", "spec/demo/contact.json,spec/demo/provisionagreement.json"
    })
    public void restoreFromThirdElementExclusive() throws Exception {
        produceSevenEntriesAndWipePersistence();
        ULID.Value txIdOfThirdElement = getTxIdOfTheNthElement(3);
        JsonNode ctx = initiateRestoreAndWaitForCompletion("default", "fromInclusive=false&from=" + txIdOfThirdElement.toString());

        // Verify
        assertEquals(countMessagesInTxLog(), 7);
        assertEquals(ctx.get("messagesRestored").longValue(), 4);
        client.get("/data/contact/4b2ef").expect404NotFound();
        client.get("/data/contact/821aa").expect404NotFound();
        client.get("/data/provisionagreement/2a41c").expect200Ok();
    }

    private ULID.Value getTxIdOfTheNthElement(int n) throws Exception {
        ULID.Value txIdOfNthElement = null;
        TxlogRawdataPool txlogRawdataPool = server.getApplication().getTxlogRawdataPool();
        RawdataClient rawdataClient = txlogRawdataPool.getClient();
        String txLogTopic = txlogRawdataPool.topicOf(null);
        try (RawdataConsumer consumer = rawdataClient.consumer(txLogTopic)) {
            for (int i = 0; i < n; i++) {
                txIdOfNthElement = consumer.receive(0, TimeUnit.MILLISECONDS).ulid();
            }
        }
        return txIdOfNthElement;
    }

    private JsonNode initiateRestoreAndWaitForCompletion(String source, String queryString) throws InterruptedException {
        String uri = "/restore/" + source + (queryString == null ? "" : ("?" + queryString));
        String snapshotState = client.post(uri).expect200Ok().body();
        JsonNode ctx = JsonTools.toJsonNode(snapshotState);
        System.out.printf("%s%n", JsonTools.toPrettyJson(ctx));

        while (!ctx.get("done").booleanValue()) {
            Thread.sleep(1000);
            snapshotState = client.get("/restore/" + source).expect200Ok().body();
            ctx = JsonTools.toJsonNode(snapshotState);
            System.out.printf("%s%n", JsonTools.toPrettyJson(ctx));
        }
        return ctx;
    }

    private void produceSevenEntriesAndWipePersistence() throws Exception {
        /*
         * Produce 7 entries in tx-log through http api
         */
        client.put("/data/provisionagreement/2a41c?sync=true&source=test&sourceId=abc123", readFileOrClasspathResource("demo/1-sirius.json")).expect201Created();
        client.put("/data/provisionagreement/2a41c/address?sync=true", readFileOrClasspathResource("demo/2-sirius-address.json")).expect200Ok();
        client.put("/data/contact/4b2ef?sync=true", readFileOrClasspathResource("demo/3-skrue.json")).expect201Created();
        client.put("/data/contact/821aa?sync=true", readFileOrClasspathResource("demo/4-donald.json")).expect201Created();
        client.put("/data/provisionagreement/2a41c/contacts/contact/4b2ef?sync=true").expect200Ok();
        client.put("/data/provisionagreement/2a41c/contacts/contact/821aa?sync=true").expect200Ok();
        client.delete("/data/contact/821aa?sync=true").expect204NoContent();

        assertEquals(countMessagesInTxLog(), 7);

        client.get("/data/contact/4b2ef").expect200Ok();
        client.get("/data/provisionagreement/2a41c").expect200Ok();
        RxJsonPersistence persistence = server.getApplication().getPersistence();
        try (Transaction tx = persistence.createTransaction(false)) {
            persistence.deleteAllEntities(tx, "data", "provisionagreement", server.getApplication().getSpecification()).blockingAwait();
            persistence.deleteAllEntities(tx, "data", "contact", server.getApplication().getSpecification()).blockingAwait();
        }
        client.get("/data/contact/4b2ef").expect404NotFound();
        client.get("/data/provisionagreement/2a41c").expect404NotFound();
    }

    private long countMessagesInTxLog() throws Exception {
        long count = 0;
        TxlogRawdataPool txlogRawdataPool = server.getApplication().getTxlogRawdataPool();
        RawdataClient rawdataClient = txlogRawdataPool.getClient();
        String txLogTopic = txlogRawdataPool.topicOf(null);
        try (RawdataConsumer consumer = rawdataClient.consumer(txLogTopic)) {
            while (consumer.receive(0, TimeUnit.MILLISECONDS) != null) {
                count++;
            }
        }
        return count;
    }
}
