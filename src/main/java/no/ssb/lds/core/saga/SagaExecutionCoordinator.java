package no.ssb.lds.core.saga;

import no.ssb.concurrent.futureselector.SelectableFuture;
import no.ssb.concurrent.futureselector.SelectableThreadPoolExectutor;
import no.ssb.saga.api.Saga;
import no.ssb.saga.execution.SagaExecution;
import no.ssb.saga.execution.SagaHandoffControl;
import no.ssb.saga.execution.SagaHandoffResult;
import no.ssb.saga.execution.adapter.AdapterLoader;
import no.ssb.saga.execution.sagalog.SagaLog;
import no.ssb.saga.execution.sagalog.SagaLogEntry;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class SagaExecutionCoordinator {

    private static final Logger LOG = LoggerFactory.getLogger(SagaExecutionCoordinator.class);

    final SagaLog sagaLog;
    final SagaRepository sagaRepository;
    final SagasObserver sagasObserver;
    final SelectableThreadPoolExectutor threadPool;
    final Semaphore semaphore;
    final ThreadPoolWatchDog threadPoolWatchDog;

    public SagaExecutionCoordinator(SagaLog sagaLog, SagaRepository sagaRepository, SagasObserver sagasObserver, SelectableThreadPoolExectutor threadPool) {
        this.sagaLog = sagaLog;
        this.sagaRepository = sagaRepository;
        this.sagasObserver = sagasObserver;
        this.threadPool = threadPool;
        int maxNumberConcurrentSagaExecutions = (threadPool.getMaximumPoolSize() + threadPool.getQueue().remainingCapacity()) / 2;
        this.semaphore = new Semaphore(maxNumberConcurrentSagaExecutions);
        threadPoolWatchDog = new ThreadPoolWatchDog();
    }

    public void startThreadpoolWatchdog() {
        threadPoolWatchDog.start();
    }

    public SagaLog getSagaLog() {
        return sagaLog;
    }

    public SelectableThreadPoolExectutor getThreadPool() {
        return threadPool;
    }

    public SelectableFuture<SagaHandoffResult> handoff(boolean sync, AdapterLoader adapterLoader, Saga saga, String namespace, String entity, String id, ZonedDateTime version, JSONObject data) {
        String versionStr = version.format(DateTimeFormatter.ISO_ZONED_DATE_TIME);
        SagaExecution sagaExecution = new SagaExecution(sagaLog, threadPool, saga, adapterLoader);
        JSONObject input = new JSONObject();
        input.put("namespace", namespace);
        input.put("entity", entity);
        input.put("id", id);
        input.put("version", versionStr);
        input.put("data", data);
        String executionId = UUID.randomUUID().toString();

        SagaHandoffControl handoffControl = startSagaExecutionWithThrottling(sagaExecution, input, executionId);

        sagasObserver.registerSaga(handoffControl);
        SelectableFuture<SagaHandoffResult> future = sync ?
                handoffControl.getCompletionFuture() : // full saga-execution
                handoffControl.getHandoffFuture();     // first saga-log write

        return future;
    }

    private SagaHandoffControl startSagaExecutionWithThrottling(SagaExecution sagaExecution, JSONObject input, String executionId) {
        SagaHandoffControl handoffControl;
        try {
            semaphore.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // set interrupt status
            throw new RuntimeException(e);
        }
        AtomicBoolean permitReleased = new AtomicBoolean(false);
        try {
            handoffControl = sagaExecution.executeSaga(executionId, input, false, r -> {
                if (permitReleased.compareAndSet(false, true)) {
                    semaphore.release();
                }
            });
            // permit will be released by sagaPermitReleaseThread
        } catch (RuntimeException e) {
            if (permitReleased.compareAndSet(false, true)) {
                semaphore.release(); // ensure that permit is always released even when saga-execution could not be run
            }
            throw e;
        }
        return handoffControl;
    }

    public void shutdown() {
        threadPoolWatchDog.shutdown();
    }

    public void recoverIncompleteSagas() {
        // TODO make reading incomplete sagas part of saga-log core
        if (!(sagaLog instanceof FileSagaLog)) {
            LOG.warn("Unable to recover incomplete Sagas due to 'none'-log!");
            return;
        }
        FileSagaLog fileSagaLog = (FileSagaLog) sagaLog;
        Map<String, Map<String, List<SagaLogEntry>>> incompleteSagasByExecutionId = fileSagaLog.readAllIncompleteSagas();
        for (Map.Entry<String, Map<String, List<SagaLogEntry>>> e : incompleteSagasByExecutionId.entrySet()) {
            startSagaRecovery(e.getKey(), e.getValue());
        }
    }

    private void startSagaRecovery(String executionId, Map<String, List<SagaLogEntry>> entriesByNodeId) {
        List<SagaLogEntry> sagaLogEntries = entriesByNodeId.get(Saga.ID_START);
        SagaLogEntry startSagaEntry = sagaLogEntries.get(0);
        Saga saga = sagaRepository.get(startSagaEntry.sagaName);
        AdapterLoader adapterLoader = sagaRepository.getAdapterLoader();
        JSONObject sagaInput = new JSONObject(startSagaEntry.jsonData);
        SagaExecution sagaExecution = new SagaExecution(sagaLog, threadPool, saga, adapterLoader);
        SagaHandoffControl handoffControl = sagaExecution.executeSaga(executionId, sagaInput, true, r -> {
        });
        LOG.info("Started recovery of saga with executionId: {}", executionId);
        sagasObserver.registerSaga(handoffControl);
    }

    /**
     * Regulary controls thread-pool to see if it is deadlocked, i.e. all threads are occupied in saga-traversal or
     * otherwise waiting for more available saga threadpool workers.
     */
    class ThreadPoolWatchDog extends Thread {

        public static final int WATCHDOG_INTERVAL_SEC = 1;

        final AtomicLong deadlockResolutionAttemptCounter = new AtomicLong(0);
        final CountDownLatch doneSignal = new CountDownLatch(1);

        public ThreadPoolWatchDog() {
            super("Saga-Threadpool-Watchdog");
        }

        void shutdown() {
            doneSignal.countDown();
        }

        @Override
        public void run() {
            try {
                BlockingQueue<Runnable> queue = threadPool.getQueue();
                int previousActiveCount = -1;
                long previousCompletedTaskCount = -1;
                while (!doneSignal.await(WATCHDOG_INTERVAL_SEC, TimeUnit.SECONDS)) {
                    // check saga-thread-pool for possible deadlock
                    if (possibleDeadlock(previousActiveCount, previousCompletedTaskCount)) {
                        int emptyTasksToSubmit = (threadPool.getMaximumPoolSize() - threadPool.getPoolSize()) / 2 + queue.remainingCapacity();
                        // Submit a number of empty tasks to thread-pool in order to force the work-queue to overflow.
                        // This should force around half the remaining thread-pool-capacity (with regards to max-size) to become available
                        LOG.info("Submitting {} empty-tasks in an attempt to resolve a potential saga-deadlock due to mismatch between workload and thread-pool configuration. Threadpool-state: {}", emptyTasksToSubmit, threadPool.toString());
                        for (int i = 0; i < emptyTasksToSubmit; i++) {
                            threadPool.submit(() -> {
                            });
                        }
                        deadlockResolutionAttemptCounter.incrementAndGet();
                    }
                    previousCompletedTaskCount = threadPool.getCompletedTaskCount();
                    previousActiveCount = threadPool.getActiveCount();
                }
            } catch (InterruptedException e) {
                LOG.warn("Saga threadpool watchdog interrupted and died.");
            }
        }

        boolean possibleDeadlock(int previousActiveCount, long previousCompletedTaskCount) {
            return threadPool.getActiveCount() > 0 // at least one active thread
                    && threadPool.getActiveCount() == threadPool.getPoolSize() // all threads in pool are active
                    && threadPool.getPoolSize() < threadPool.getMaximumPoolSize() // pool can still grow
                    && threadPool.getActiveCount() == previousActiveCount // no recent change in activity
                    && threadPool.getCompletedTaskCount() == previousCompletedTaskCount; // no more tasks have completed since previous check
        }
    }
}
