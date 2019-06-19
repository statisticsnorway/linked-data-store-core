package no.ssb.lds.core.saga;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import no.ssb.concurrent.futureselector.SelectableFuture;
import no.ssb.concurrent.futureselector.SelectableThreadPoolExectutor;
import no.ssb.lds.api.persistence.json.JsonTools;
import no.ssb.saga.api.Saga;
import no.ssb.saga.execution.SagaExecution;
import no.ssb.saga.execution.SagaHandoffControl;
import no.ssb.saga.execution.SagaHandoffResult;
import no.ssb.saga.execution.adapter.AdapterLoader;
import no.ssb.sagalog.SagaLog;
import no.ssb.sagalog.SagaLogAlreadyAquiredByOtherOwnerException;
import no.ssb.sagalog.SagaLogEntry;
import no.ssb.sagalog.SagaLogId;
import no.ssb.sagalog.SagaLogOwner;
import no.ssb.sagalog.SagaLogPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.stream.Collectors.groupingBy;
import static no.ssb.lds.api.persistence.json.JsonTools.mapper;

public class SagaExecutionCoordinator {

    private static final Logger LOG = LoggerFactory.getLogger(SagaExecutionCoordinator.class);

    final int numberOfSagaLogs;
    final SagaLogPool sagaLogPool;
    final Set<SagaLogId> allLogIds;
    final BlockingQueue<SagaLogId> availableLogIds = new LinkedBlockingQueue<>();
    final Map<SagaLogId, SagaLog> sagaLogBySagaLogId = new ConcurrentHashMap<>();
    final Map<SagaLogId, String> executionIdBySagaLogId = new ConcurrentHashMap<>();
    final boolean sagaCommandsEnabled;

    final SagaRepository sagaRepository;
    final SagasObserver sagasObserver;
    final SelectableThreadPoolExectutor threadPool;
    final Semaphore semaphore;
    final ThreadPoolWatchDog threadPoolWatchDog;

    public SagaExecutionCoordinator(SagaLogPool sagaLogPool, int numberOfSagaLogs, SagaRepository sagaRepository, SagasObserver sagasObserver, SelectableThreadPoolExectutor threadPool, boolean sagaCommandsEnabled) {
        this.sagaLogPool = sagaLogPool;
        this.numberOfSagaLogs = numberOfSagaLogs;
        this.sagaCommandsEnabled = sagaCommandsEnabled;
        Set<SagaLogId> allLogIds = new LinkedHashSet<>();
        for (int i = 0; i < numberOfSagaLogs; i++) {
            SagaLogId logId = sagaLogPool.idFor(String.format("%02d", i));
            allLogIds.add(logId);
        }
        this.allLogIds = Collections.unmodifiableSet(allLogIds);
        availableLogIds.addAll(allLogIds);
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

    public SagaLogPool getSagaLogPool() {
        return sagaLogPool;
    }

    public SelectableThreadPoolExectutor getThreadPool() {
        return threadPool;
    }

    public SelectableFuture<SagaHandoffResult> handoff(boolean sync, AdapterLoader adapterLoader, Saga saga, String namespace, String entity, String id, ZonedDateTime version, JsonNode data, Map<String, List<SagaCommand>> commandsByNodeId) {
        String versionStr = version.format(DateTimeFormatter.ISO_ZONED_DATE_TIME);
        ObjectNode input = mapper.createObjectNode();
        input.put("namespace", namespace);
        input.put("entity", entity);
        input.put("id", id);
        input.put("version", versionStr);
        input.set("data", data);
        String executionId = UUID.randomUUID().toString();

        SagaLogId sagaLogId = getAvailableLogId();
        SagaLog sagaLog = acquireSagaLogBlocking(sagaLogId);
        executionIdBySagaLogId.compute(sagaLogId, (k, v) -> {
            if (v != null) {
                throw new RuntimeException(String.format("executionIdBySagaLogId with key %s is already associated with another executionId %s", k, v));
            }
            return executionId;
        });
        sagaLogBySagaLogId.compute(sagaLogId, (k, v) -> {
            if (v != null) {
                throw new RuntimeException(String.format("sagaLogBySagaLogId with key %s is already associated with another value", k));
            }
            return sagaLog;
        });
        SagaExecution sagaExecution = new SagaExecution(sagaLog, threadPool, saga, adapterLoader);

        SagaHandoffControl handoffControl = startSagaExecutionWithThrottling(sagaExecution, input, executionId, sagaLog, sagaCommandsEnabled ? commandsByNodeId : Collections.emptyMap());

        sagasObserver.registerSaga(handoffControl);
        SelectableFuture<SagaHandoffResult> future = sync ?
                handoffControl.getCompletionFuture() : // full saga-execution
                handoffControl.getHandoffFuture();     // first saga-log write

        return future;
    }

    private SagaLog acquireSagaLogBlocking(SagaLogId sagaLogId) {
        SagaLog sagaLog = null;
        int acquireAttempts = 0;
        do {
            acquireAttempts++;
            try {
                sagaLog = sagaLogPool.acquire(new SagaLogOwner("Thread::" + Thread.currentThread().getName()), sagaLogId);
            } catch (SagaLogAlreadyAquiredByOtherOwnerException e) {
                try {
                    Thread.sleep(100 * (int) Math.min(100, Math.pow(2, acquireAttempts)));
                } catch (InterruptedException ex) {
                    throw new RuntimeException(ex);
                }
            }
        } while (sagaLog == null);
        return sagaLog;
    }

    SagaLogId getAvailableLogId() {
        try {
            return availableLogIds.take();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    Set<SagaLogId> getAllLogIds() {
        return allLogIds;
    }

    private SagaHandoffControl startSagaExecutionWithThrottling(SagaExecution sagaExecution, JsonNode input, String executionId, SagaLog sagaLog, Map<String, List<SagaCommand>> commandsByNodeId) {
        SagaHandoffControl handoffControl;
        try {
            semaphore.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // set interrupt status
            throw new RuntimeException(e);
        }
        AtomicBoolean permitReleased = new AtomicBoolean(false);
        try {
            handoffControl = sagaExecution.executeSaga(executionId, input, false,
                    r -> {
                        if (r.isSuccess()) {
                            sagaLog.truncate().join();
                        }
                        if (permitReleased.compareAndSet(false, true)) {
                            semaphore.release();
                        }
                        sagaLogBySagaLogId.remove(sagaLog.id());
                        executionIdBySagaLogId.remove(sagaLog.id());
                        sagaLogPool.release(sagaLog.id());
                        availableLogIds.add(sagaLog.id());
                    },
                    sagaExecutionTraversalContext -> {
                        List<SagaCommand> commands = commandsByNodeId.get(sagaExecutionTraversalContext.getNode().id);
                        if (commands == null) {
                            return;
                        }
                        for (SagaCommand command : commands) {
                            String cmd = command.getCommand();
                            if ("failBefore".equalsIgnoreCase(cmd)) {
                                throw new RuntimeException("failBefore saga command");
                            }
                        }
                    },
                    sagaExecutionTraversalContext -> {
                        List<SagaCommand> commands = commandsByNodeId.get(sagaExecutionTraversalContext.getNode().id);
                        if (commands == null) {
                            return;
                        }
                        for (SagaCommand command : commands) {
                            String cmd = command.getCommand();
                            if ("failAfter".equalsIgnoreCase(cmd)) {
                                throw new RuntimeException("failAfter saga command");
                            }
                        }
                    }
            );
            // permit will be released by sagaPermitReleaseThread
        } catch (RuntimeException e) {
            if (permitReleased.compareAndSet(false, true)) {
                semaphore.release(); // ensure that permit is always released even when saga-execution could not be run
            }
            sagaLogBySagaLogId.remove(sagaLog.id());
            executionIdBySagaLogId.remove(sagaLog.id());
            sagaLogPool.release(sagaLog.id());
            availableLogIds.add(sagaLog.id());
            throw e;
        }
        return handoffControl;
    }

    public void shutdown() {
        threadPoolWatchDog.shutdown();
    }

    public CompletableFuture<Void> completeClusterWideIncompleteSagas(ExecutorService executorService) {
        Set<SagaLogId> logIds = sagaLogPool.clusterWideLogIds();
        Set<SagaLogId> instanceLocalLogIds = sagaLogPool.instanceLocalLogIds();
        LinkedHashSet<SagaLogId> nonLocalClusterSagaLogs = new LinkedHashSet<>(logIds);
        nonLocalClusterSagaLogs.removeAll(instanceLocalLogIds);

        List<CompletableFuture<CompletableFuture<Void>>> tasks = new ArrayList<>();
        for (SagaLogId logId : logIds) {
            try {
                tasks.add(CompletableFuture.supplyAsync(() -> doCompleteSagaLog(logId, nonLocalClusterSagaLogs.contains(logId)), executorService));
            } catch (Throwable t) {
                LOG.warn(String.format("Error while attempting to complete saga from saga-log: %s", logId), t);
            }
        }
        CompletableFuture.allOf(tasks.toArray(new CompletableFuture[tasks.size()])).join(); // wait for executorService
        List<CompletableFuture<Void>> innerTasks = new ArrayList<>();
        for (CompletableFuture<CompletableFuture<Void>> task : tasks) {
            innerTasks.add(task.join()); // will not block
        }
        return CompletableFuture.allOf(innerTasks.toArray(new CompletableFuture[innerTasks.size()]));
    }

    private CompletableFuture<Void> doCompleteSagaLog(SagaLogId logId, boolean removeFromPoolWhenDone) {
        SagaLog sagaLog;
        try {
            sagaLog = sagaLogPool.acquire(new SagaLogOwner("Thread::" + Thread.currentThread().getName()), logId);
        } catch (SagaLogAlreadyAquiredByOtherOwnerException e) {
            if (removeFromPoolWhenDone) {
                sagaLogPool.remove(logId);
            }
            return CompletableFuture.completedFuture(null);
        }
        Map<String, List<SagaLogEntry>> entriesByExecutionId = sagaLog.readIncompleteSagas().collect(groupingBy(SagaLogEntry::getExecutionId));
        if (entriesByExecutionId.isEmpty()) {
            availableLogIds.add(logId);
            sagaLogPool.release(logId);
            if (removeFromPoolWhenDone) {
                sagaLogPool.remove(logId);
            }
            return CompletableFuture.completedFuture(null);
        }
        List<CompletableFuture<Void>> futureList = new LinkedList<>();
        for (Map.Entry<String, List<SagaLogEntry>> entry : entriesByExecutionId.entrySet()) {
            String executionId = entry.getKey();
            List<SagaLogEntry> entries = entry.getValue();
            Map<String, List<SagaLogEntry>> entriesByNodeId = entries.stream().collect(groupingBy(SagaLogEntry::getNodeId));
            try {
                futureList.add(startSagaForwardRecovery(executionId, entriesByNodeId, sagaLog));
            } catch (Throwable t) {
                availableLogIds.add(logId);
                sagaLogPool.release(logId);
                if (removeFromPoolWhenDone) {
                    sagaLogPool.remove(logId);
                }
                return CompletableFuture.failedFuture(t);
            }
        }
        return CompletableFuture.allOf(futureList.toArray(new CompletableFuture[futureList.size()]))
                .whenComplete((v, t) -> {
                    if (t != null) {
                        availableLogIds.add(logId);
                        sagaLogPool.release(logId);
                        if (removeFromPoolWhenDone) {
                            sagaLogPool.remove(logId);
                        }
                        LOG.warn("Unable to complete saga forward recovery", t);
                        return;
                    }
                    sagaLog.truncate().join();
                    availableLogIds.add(logId);
                    sagaLogPool.release(logId);
                    if (removeFromPoolWhenDone) {
                        sagaLogPool.remove(logId);
                    }
                });
    }

    private CompletableFuture<Void> startSagaForwardRecovery(String executionId, Map<String, List<SagaLogEntry>> entriesByNodeId, SagaLog sagaLog) {
        if (entriesByNodeId.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        List<SagaLogEntry> sagaLogEntries = entriesByNodeId.get(Saga.ID_START);
        if (sagaLogEntries == null) {
            throw new RuntimeException(String.format("In saga-log %s, Missing Saga start id. Entries: %s", sagaLog.id(), entriesByNodeId));
        }
        SagaLogEntry startSagaEntry = sagaLogEntries.get(0);
        Saga saga = sagaRepository.get(startSagaEntry.getSagaName());
        if (saga == null) {
            throw new RuntimeException(String.format("In saga-log %s, Saga with name %s is not present in sagaRepository", sagaLog.id(), startSagaEntry.getSagaName()));
        }
        AdapterLoader adapterLoader = sagaRepository.getAdapterLoader();
        JsonNode sagaInput = JsonTools.toJsonNode(startSagaEntry.getJsonData());
        SagaExecution sagaExecution = new SagaExecution(sagaLog, threadPool, saga, adapterLoader);
        CompletableFuture<SagaHandoffResult> future = new CompletableFuture<>();
        SagaHandoffControl handoffControl = sagaExecution.executeSaga(executionId, sagaInput, true, r -> future.complete(r));
        sagasObserver.registerSaga(handoffControl);
        LOG.info("Started recovery of saga with sagaLog: {} and executionId: {}", sagaLog.id(), executionId);
        return future.thenCompose(r -> {
            if (r.isFailure()) {
                LOG.info("Recovery of saga failed, sagaLog: {}, executionId: {}", sagaLog.id(), executionId);
                return CompletableFuture.failedFuture(r.getFailureCause()); // unwrap
            } else {
                LOG.info("Recovery of saga succeeded, sagaLog: {}, executionId: {}", sagaLog.id(), executionId);
                return CompletableFuture.completedFuture(null);
            }
        });
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
            } catch (Throwable t) {
                LOG.warn("", t);
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
