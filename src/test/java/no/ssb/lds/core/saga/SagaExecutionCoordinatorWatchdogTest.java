package no.ssb.lds.core.saga;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import no.ssb.lds.api.persistence.json.JsonDocument;
import no.ssb.lds.core.persistence.PersistenceCreateOrOverwriteSagaAdapter;
import no.ssb.lds.core.utils.FileAndClasspathReaderUtils;
import no.ssb.lds.test.ConfigurationOverride;
import no.ssb.lds.test.client.TestClient;
import no.ssb.lds.test.server.TestServer;
import no.ssb.lds.test.server.TestServerListener;
import no.ssb.saga.api.Saga;
import no.ssb.saga.execution.adapter.Adapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import javax.inject.Inject;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.testng.Assert.assertEquals;

@Listeners(TestServerListener.class)
public class SagaExecutionCoordinatorWatchdogTest {

    private static final Logger LOG = LoggerFactory.getLogger(SagaExecutionCoordinatorWatchdogTest.class);

    @Inject
    TestServer server;

    @Inject
    TestClient client;

    /**
     * Configurations where saga.threadpool.queue.capacity >= saga.threadpool.core
     * will most likely provoke a deadlock.
     */
    @Test
    @ConfigurationOverride({
            "transaction.log.enabled", "false",
            "persistence.provider", "mem",
            "saga.log.type", "none",
            "specification.schema", "spec/schemas/contact.json,spec/schemas/provisionagreement.json",
            "saga.threadpool.core", "15",
            "saga.threadpool.max", "50",
            "saga.threadpool.queue.capacity", "100",
    })
    public void thatWatchdogIsTriggeredWhenSagaThreadpoolIsDeadlocked() throws InterruptedException {
        SagaExecutionCoordinator sec = server.getSagaExecutionCoordinator();

        {
            /*
             * Configure a  dummy slow node before saga fan-out in order to easily provoke deadlock.
             */
            final ObjectNode empty = JsonDocument.mapper.createObjectNode();
            sec.sagaRepository.getAdapterLoader().register(new Adapter<>(JsonNode.class, "SlowNodeAdapter", (i, d) -> {
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                }
                return empty;
            }));
            sec.sagaRepository.getAdapterLoader().register(new Adapter<>(JsonNode.class, "search", (i, d) -> empty));
            sec.sagaRepository.register(Saga
                    .start(SagaRepository.SAGA_CREATE_OR_UPDATE_MANAGED_RESOURCE).linkTo("slownode", "search-index-update")
                    .id("slownode").adapter("SlowNodeAdapter").linkTo("persistence")
                    .id("persistence").adapter(PersistenceCreateOrOverwriteSagaAdapter.NAME).linkToEnd()
                    .id("search-index-update").adapter("search").linkToEnd()
                    .end());
        }

        int sagaThreadPoolCoreSize = sec.threadPool.getCorePoolSize();

        final ExecutorService executor = Executors.newFixedThreadPool(sagaThreadPoolCoreSize);

        try {
            JsonNode provisionAgreementSirius = resource("provisionagreement_sirius.json");

            AtomicInteger successfulWrites = new AtomicInteger(0);
            LOG.debug("Running {} sagas simultaneously", sagaThreadPoolCoreSize);
            for (int i = 0; i < sagaThreadPoolCoreSize; i++) {
                executor.submit(() -> {
                    client.put("/data/provisionagreement/100", provisionAgreementSirius.toString()).expect200Ok();
                    successfulWrites.incrementAndGet();
                });
            }

            LOG.debug("Waiting for deadlock to settle");
            Thread.sleep(300);

            {
                LOG.debug("Confirm that there is a deadlock");
                assertEquals(sec.threadPoolWatchDog.deadlockResolutionAttemptCounter.get(), 0);
                int previousActiveCount;
                long previousCompletedTaskCount;
                int i = 0;
                do {
                    if (i++ >= 5) {
                        Assert.fail("Unable to provoke deadlock within 5 seconds, failing!");
                    }
                    previousActiveCount = sec.threadPool.getActiveCount();
                    previousCompletedTaskCount = sec.threadPool.getCompletedTaskCount();
                    LOG.debug("Threadpool-state: {}", sec.threadPool.toString());
                    Thread.sleep(TimeUnit.SECONDS.toMillis(1));
                } while (!sec.threadPoolWatchDog.possibleDeadlock(previousActiveCount, previousCompletedTaskCount));
                LOG.debug("Threadpool-state: {}", sec.threadPool.toString());
                LOG.debug("DEADLOCKED!");
            }

            LOG.debug("Starting saga watch-dog");
            sec.startThreadpoolWatchdog();

            {
                LOG.debug("Waiting for deadlock resolution attempt");
                int i = 0;
                do {
                    Thread.sleep(1000);
                    if (++i >= 5) {
                        Assert.fail("Unable to detect deadlock resolution attempt within 10 seconds, failing!");
                    }
                    LOG.debug("Threadpool-state: {}", sec.threadPool.toString());
                } while (sec.threadPoolWatchDog.deadlockResolutionAttemptCounter.get() == 0);
            }

            LOG.debug("Waiting for sagas to complete after deadlock resolution attempt");
            shutdownAndAwaitTermination(executor);

            LOG.debug("Confirming that all sagas are complete after deadlock");
            assertEquals(successfulWrites.get(), sagaThreadPoolCoreSize);
        } finally {
            if (!executor.isShutdown()) {
                shutdownAndAwaitTermination(executor);
            }
        }
    }

    static final JsonNode resource(String resourceName) {
        try {
            return JsonDocument.mapper.readTree(FileAndClasspathReaderUtils.getResourceAsString("spec/schemas.examples/" + resourceName, StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    static void shutdownAndAwaitTermination(ExecutorService pool) {
        shutdownAndAwaitTermination(pool, 5, TimeUnit.SECONDS);
    }

    static void shutdownAndAwaitTermination(ExecutorService pool, int timeout, TimeUnit unit) {
        pool.shutdown();
        try {
            if (!pool.awaitTermination(timeout, unit)) {
                pool.shutdownNow();
                if (!pool.awaitTermination(10, TimeUnit.SECONDS)) {
                    System.err.println("Pool did not terminate");
                }
            }
        } catch (InterruptedException ie) {
            pool.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}

