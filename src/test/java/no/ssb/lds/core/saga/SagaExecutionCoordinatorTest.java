package no.ssb.lds.core.saga;

import no.ssb.lds.test.ConfigurationOverride;
import no.ssb.lds.test.client.TestClient;
import no.ssb.lds.test.server.TestServer;
import no.ssb.lds.test.server.TestServerListener;
import no.ssb.saga.execution.SagaExecutionTraversalContext;
import no.ssb.sagalog.SagaLog;
import no.ssb.sagalog.SagaLogOwner;
import no.ssb.sagalog.SagaLogPool;
import org.testng.Assert;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import javax.inject.Inject;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Listeners(TestServerListener.class)
public class SagaExecutionCoordinatorTest {

    @Inject
    TestClient client;

    @Inject
    TestServer server;


    @Test
    @ConfigurationOverride({
            "saga.recovery.enabled", "false",
            "sagalog.provider", "no.ssb.sagalog.file.FileSagaLogInitializer",
            "saga.number-of-logs", "1",
            "saga.commands.enabled", "true"
    })
    public void thatSagaRecoveryWorksWhenAllLogsAreClean() {
        SagaExecutionCoordinator sec = server.getSagaExecutionCoordinator();
        {
            // force a visible non-local saga-log in cluster to trigger more code-paths
            SagaLogPool pool = sec.getSagaLogPool();
            SagaLog otherClusterIdLog = pool.connect(pool.idFor("other", "x"));
            pool.remove(otherClusterIdLog.id());
        }
        sec.completeClusterWideIncompleteSagas(server.getSagaExecutionCoordinator().recoveryThreadPool);
    }

    @Test
    @ConfigurationOverride({
            "saga.recovery.enabled", "false",
            "sagalog.provider", "no.ssb.sagalog.memory.MemorySagaLogInitializer",
            "saga.number-of-logs", "1",
            "saga.commands.enabled", "true"
    })
    public void testThatInstanceLocalSagaExecutionIsRecoveredOnAcquireClean() {
        SagaExecutionCoordinator sec = server.getSagaExecutionCoordinator();
        SagaLogPool pool = sec.getSagaLogPool();
        SagaLog deadSagaLog = pool.connect(sec.deadSagaLogId);
        deadSagaLog.truncate();

        client.put("/data/provisionagreement/m1?sync=true&saga=failAfter%20S", "{\"name\":\"saga-handoff-test-data 1\",\"contacts\":[]}").expectAnyOf(500);

        // check state
        SagaLogOwner owner = new SagaLogOwner("core-sec-test");
        SagaLog sagaLog = pool.tryAcquire(owner);
        try {
            assertTrue(sagaLog.readIncompleteSagas().anyMatch(e -> true)); // check that saga-log is not empty
            assertFalse(deadSagaLog.readIncompleteSagas().anyMatch(e -> true)); // check that dead-saga-log is empty
        } finally {
            pool.release(sagaLog.id());
        }

        client.put("/data/provisionagreement/m1?sync=true", "{\"name\":\"saga-handoff-test-data 2\",\"contacts\":[]}").expect201Created();

        pool.tryTakeOwnership(owner, sagaLog.id());
        try {
            assertFalse(sagaLog.readIncompleteSagas().anyMatch(e -> true)); // check that saga-log is empty
            assertFalse(deadSagaLog.readIncompleteSagas().anyMatch(e -> true)); // check that dead-saga-log is empty
        } finally {
            pool.releaseOwnership(sagaLog.id());
        }
    }

    @Test
    @ConfigurationOverride({
            "saga.recovery.enabled", "false",
            "sagalog.provider", "no.ssb.sagalog.memory.MemorySagaLogInitializer",
            "saga.number-of-logs", "1",
            "saga.commands.enabled", "true"
    })
    public void testThatInstanceLocalSagaEntriesAreMovedToDeadSagaLogOnAcquireCleanWhenSagaExecutionFailureRepeats() {
        SagaExecutionCoordinator sec = server.getSagaExecutionCoordinator();
        SagaLogPool pool = sec.getSagaLogPool();
        SagaLog deadSagaLog = pool.connect(sec.deadSagaLogId);
        deadSagaLog.truncate();

        client.put("/data/provisionagreement/m1?sync=true&saga=failAfter%20S", "{\"name\":\"saga-handoff-test-data\",\"contacts\":[]}").expectAnyOf(500);

        try {
            // check state
            SagaLogOwner owner = new SagaLogOwner("core-sec-test");
            SagaLog sagaLog = pool.tryAcquire(owner);
            try {
                assertTrue(sagaLog.readIncompleteSagas().anyMatch(e -> true)); // check that saga-log is not empty
                assertFalse(deadSagaLog.readIncompleteSagas().anyMatch(e -> true)); // check that dead-saga-log is empty
            } finally {
                pool.release(sagaLog.id());
            }

            SagaLog otherSagaLog = sec.acquireCleanSagaLog(nopAction(), failOnce()); // should move the entries to dead-saga log
            try {
                Assert.assertEquals(otherSagaLog, sagaLog);
                sagaLog = otherSagaLog;

                assertFalse(sagaLog.readIncompleteSagas().anyMatch(e -> true)); // check that saga-log is empty
                assertTrue(deadSagaLog.readIncompleteSagas().anyMatch(e -> true)); // check that dead-saga-log is not empty
            } finally {
                pool.release(otherSagaLog.id());
            }
        } finally {
            deadSagaLog.truncate();
        }
    }

    private Consumer<SagaExecutionTraversalContext> failOnce() {
        AtomicBoolean failedOnce = new AtomicBoolean();
        return c -> {
            if (!failedOnce.compareAndSet(false, true)) {
                return;
            }
            throw new RuntimeException(String.format("Forced repeat error initiated from test at sagaNode %s", c.getNode().id));
        };
    }

    private Consumer<SagaExecutionTraversalContext> nopAction() {
        return c -> {
        };
    }
}