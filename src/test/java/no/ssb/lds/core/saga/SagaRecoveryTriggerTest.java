package no.ssb.lds.core.saga;

import no.ssb.concurrent.futureselector.SelectableFuture;
import no.ssb.lds.api.persistence.json.JsonTools;
import no.ssb.lds.test.ConfigurationOverride;
import no.ssb.lds.test.server.TestServer;
import no.ssb.lds.test.server.TestServerListener;
import no.ssb.saga.api.Saga;
import no.ssb.saga.api.SagaNode;
import no.ssb.saga.execution.SagaHandoffResult;
import no.ssb.saga.execution.adapter.AbortSagaException;
import no.ssb.saga.execution.adapter.Adapter;
import no.ssb.sagalog.SagaLog;
import no.ssb.sagalog.SagaLogId;
import no.ssb.sagalog.SagaLogPool;
import org.testng.Assert;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import javax.inject.Inject;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;

@Listeners(TestServerListener.class)
public class SagaRecoveryTriggerTest {

    @Inject
    TestServer server;

    @Test
    @ConfigurationOverride({
            "saga.recovery.enabled", "false",
            "sagalog.provider", "no.ssb.sagalog.memory.MemorySagaLogInitializer"
    })
    public void thatSomethingHappens() throws InterruptedException {
        SagaExecutionCoordinator sec = server.getSagaExecutionCoordinator();
        sec.sagaRepository.getAdapterLoader().register(new FailingTheFirstTimeSagaAdapter());
        Saga saga = Saga.start("ZigZagSaga").linkTo("zigzag").id("zigzag").adapter("zigzag").linkToEnd().end();
        sec.sagaRepository.register(saga);
        SagaLogPool sagaLogPool = sec.getSagaLogPool();
        SelectableFuture<SagaHandoffResult> handoff = sec.handoff(true, sec.sagaRepository.getAdapterLoader(), saga,
                "ns", "SomeEntity", "x123", ZonedDateTime.now(), JsonTools.mapper.createObjectNode(), Collections.emptyMap());
        try {
            handoff.join(); // wait for saga-execution to fail
            Assert.fail("Saga execution did no fail on first attempt");
        } catch (CompletionException e) {
            // expected, ignore
        }

        // there is now a failed saga in saga-log

        SagaRecoveryTrigger sagaRecoveryTrigger = new SagaRecoveryTrigger(sec, 1, 1);
        sagaRecoveryTrigger.start();

        try {
            System.out.printf("Waiting for saga-recovery%n");

            for (int i = 0; ; i++) {
                Thread.sleep(1000);
                int incompleteEntryCount = 0;
                for (SagaLogId logId : sec.getAllLogIds()) {
                    SagaLog sagaLog = sagaLogPool.connect(logId);
                    incompleteEntryCount += sagaLog.readIncompleteSagas().count();
                }
                if (incompleteEntryCount == 0) {
                    break; // successfully recovered saga
                }
                if (i >= 5) {
                    Assert.fail("Timeout, saga was not recovered in time");
                }
            }

        } finally {
            sagaRecoveryTrigger.stop();
        }
    }

    static class FailingTheFirstTimeSagaAdapter extends Adapter<String> {
        final AtomicInteger attempts = new AtomicInteger();

        FailingTheFirstTimeSagaAdapter() {
            super(String.class, "zigzag");
        }

        @Override
        public String executeAction(SagaNode sagaNode, Object sagaInput, Map<SagaNode, Object> dependeesOutput) throws AbortSagaException {
            int attempt = attempts.incrementAndGet();
            if (attempt == 1) {
                throw new RuntimeException("Failing because this is attempt # " + attempt);
            }
            return "OK";
        }
    }
}