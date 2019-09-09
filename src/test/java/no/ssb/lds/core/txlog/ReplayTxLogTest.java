package no.ssb.lds.core.txlog;

import no.ssb.lds.core.saga.SagaInput;
import no.ssb.lds.test.ConfigurationOverride;
import no.ssb.lds.test.client.TestClient;
import no.ssb.lds.test.server.TestServer;
import no.ssb.lds.test.server.TestServerListener;
import no.ssb.rawdata.api.RawdataClient;
import no.ssb.rawdata.api.RawdataConsumer;
import no.ssb.rawdata.api.RawdataMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import javax.inject.Inject;
import java.util.concurrent.TimeUnit;

import static no.ssb.lds.core.utils.FileAndClasspathReaderUtils.readFileOrClasspathResource;
import static org.testng.Assert.assertEquals;

@Listeners(TestServerListener.class)
public class ReplayTxLogTest {

    Logger log = LoggerFactory.getLogger(ReplayTxLogTest.class);

    @Inject
    private TestClient client;

    @Inject
    private TestServer server;

    @Test
    @ConfigurationOverride({
            "txlog.rawdata.provider", "memory",
            "specification.schema", "spec/demo/contact.json,spec/demo/provisionagreement.json"
    })
    public void replayTxLogAndVerify() throws Exception {

        /*
         * Produce 7 entries in tx-log through http api
         */
        client.put("/data/provisionagreement/2a41c?sync=true", readFileOrClasspathResource("demo/1-sirius.json")).expect201Created();
        client.put("/data/provisionagreement/2a41c/address?sync=true", readFileOrClasspathResource("demo/2-sirius-address.json")).expect200Ok();
        client.put("/data/contact/4b2ef?sync=true", readFileOrClasspathResource("demo/3-skrue.json")).expect201Created();
        client.put("/data/contact/821aa?sync=true", readFileOrClasspathResource("demo/4-donald.json")).expect201Created();
        client.put("/data/provisionagreement/2a41c/contacts/contact/4b2ef?sync=true").expect200Ok();
        client.put("/data/provisionagreement/2a41c/contacts/contact/821aa?sync=true").expect200Ok();
        client.delete("/data/provisionagreement/2a41c/contacts/contact/4b2ef?sync=true").expect200Ok();

        dumpCurrentTxLog(); // for debugging purposes

        /*
         * Read and check that the tx-log contains exactly the 7 elements in the correct order
         */
        RawdataClient rawdataClient = server.getApplication().getTxLogClient();
        String txLogTopic = server.getConfiguration().evaluateToString("txlog.rawdata.topic");

        try (RawdataConsumer consumer = rawdataClient.consumer(txLogTopic)) {

            RawdataMessage m1 = consumer.receive(0, TimeUnit.MILLISECONDS);
            assertEquals(entityAndId(m1), "provisionagreement/2a41c");
            assertEquals(TxLogTools.txEntryToSagaInput(m1).method(), "PUT");

            RawdataMessage m2 = consumer.receive(0, TimeUnit.MILLISECONDS);
            assertEquals(entityAndId(m2), "provisionagreement/2a41c");
            assertEquals(TxLogTools.txEntryToSagaInput(m2).method(), "PUT");

            RawdataMessage m3 = consumer.receive(0, TimeUnit.MILLISECONDS);
            assertEquals(entityAndId(m3), "contact/4b2ef");
            assertEquals(TxLogTools.txEntryToSagaInput(m3).method(), "PUT");

            RawdataMessage m4 = consumer.receive(0, TimeUnit.MILLISECONDS);
            assertEquals(entityAndId(m4), "contact/821aa");
            assertEquals(TxLogTools.txEntryToSagaInput(m4).method(), "PUT");

            RawdataMessage m5 = consumer.receive(0, TimeUnit.MILLISECONDS);
            assertEquals(entityAndId(m5), "provisionagreement/2a41c");
            assertEquals(TxLogTools.txEntryToSagaInput(m5).method(), "PUT");

            RawdataMessage m6 = consumer.receive(0, TimeUnit.MILLISECONDS);
            assertEquals(entityAndId(m6), "provisionagreement/2a41c");
            assertEquals(TxLogTools.txEntryToSagaInput(m6).method(), "PUT");

            RawdataMessage m7 = consumer.receive(0, TimeUnit.MILLISECONDS);
            assertEquals(entityAndId(m7), "provisionagreement/2a41c");
            assertEquals(TxLogTools.txEntryToSagaInput(m7).method(), "PUT");
        }
    }

    private String entityAndId(RawdataMessage m1) {
        String position = m1.position();
        return position.substring(0, position.length() - 14); // remove the last 14 characters in order to strip away the version/timestamp component
    }

    private void dumpCurrentTxLog() throws Exception {
        RawdataClient rawdataClient = server.getApplication().getTxLogClient();
        String txLogTopic = server.getConfiguration().evaluateToString("txlog.rawdata.topic");
        try (RawdataConsumer consumer = rawdataClient.consumer(txLogTopic)) {
            RawdataMessage message;
            for (int i = 1; (message = consumer.receive(0, TimeUnit.MILLISECONDS)) != null; i++) {
                SagaInput sagaInput = TxLogTools.txEntryToSagaInput(message);
                log.debug("TxLog Entry # {}:\n{}", i, sagaInput.toPrettyString());
            }
        }
    }
}
