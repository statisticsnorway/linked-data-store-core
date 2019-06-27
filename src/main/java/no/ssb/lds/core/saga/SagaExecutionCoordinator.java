package no.ssb.lds.core.saga;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import no.ssb.concurrent.futureselector.SelectableFuture;
import no.ssb.concurrent.futureselector.SelectableThreadPoolExectutor;
import no.ssb.lds.api.persistence.json.JsonTools;
import no.ssb.saga.api.Saga;
import no.ssb.saga.execution.SagaExecution;
import no.ssb.saga.execution.SagaExecutionTraversalContext;
import no.ssb.saga.execution.SagaHandoffControl;
import no.ssb.saga.execution.SagaHandoffResult;
import no.ssb.saga.execution.adapter.AdapterLoader;
import no.ssb.sagalog.SagaLog;
import no.ssb.sagalog.SagaLogAlreadyAquiredByOtherOwnerException;
import no.ssb.sagalog.SagaLogBusyException;
import no.ssb.sagalog.SagaLogEntry;
import no.ssb.sagalog.SagaLogEntryBuilder;
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
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.regex.Pattern;

import static java.util.stream.Collectors.groupingBy;
import static no.ssb.lds.api.persistence.json.JsonTools.mapper;

public class SagaExecutionCoordinator {

    private static final Logger LOG = LoggerFactory.getLogger(SagaExecutionCoordinator.class);

    static final Pattern deadLetterSagaPattern = Pattern.compile("dead-saga");

    final int numberOfSagaLogs;
    final SagaLogPool sagaLogPool;
    final SagaLogId deadSagaLogId;
    final Map<SagaLogId, String> executionIdBySagaLogId = new ConcurrentHashMap<>();
    final boolean sagaCommandsEnabled;

    final SagaRepository sagaRepository;
    final SagasObserver sagasObserver;
    final SelectableThreadPoolExectutor threadPool;
    final Semaphore semaphore;
    final ThreadPoolWatchDog threadPoolWatchDog;
    final ExecutorService recoveryThreadPool;

    public SagaExecutionCoordinator(SagaLogPool sagaLogPool, int numberOfSagaLogs, SagaRepository sagaRepository, SagasObserver sagasObserver, SelectableThreadPoolExectutor threadPool, boolean sagaCommandsEnabled, ExecutorService recoveryThreadPool) {
        this.sagaLogPool = sagaLogPool;
        this.numberOfSagaLogs = numberOfSagaLogs;
        this.sagaCommandsEnabled = sagaCommandsEnabled;
        this.recoveryThreadPool = recoveryThreadPool;
        this.deadSagaLogId = sagaLogPool.idFor(sagaLogPool.getLocalClusterInstanceId(), "dead-saga"); // do not register

        if (!deadLetterSagaPattern.matcher(deadSagaLogId.getLogName()).matches()) {
            throw new IllegalStateException(String.format("Unable to match dead-letter-saga log %s with pattern: %s", deadSagaLogId, deadLetterSagaPattern.pattern()));
        }
        Set<SagaLogId> allLogIds = new LinkedHashSet<>();
        for (int i = 0; i < numberOfSagaLogs; i++) {
            SagaLogId logId = sagaLogPool.registerInstanceLocalIdFor(String.format("%02d", i));
            if (deadLetterSagaPattern.matcher(logId.getLogName()).matches()) {
                throw new IllegalStateException(String.format("Unwanted match to of log %s with registered dead-letter-saga pattern: %s", logId, deadLetterSagaPattern.pattern()));
            }
            allLogIds.add(logId);
        }
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

        SagaLog sagaLog = acquireCleanSagaLog(c -> {
        }, c -> {
        });
        executionIdBySagaLogId.compute(sagaLog.id(), (k, v) -> {
            if (v != null) {
                throw new RuntimeException(String.format("executionIdBySagaLogId with key %s is already associated with another executionId %s", k, v));
            }
            return executionId;
        });
        SagaExecution sagaExecution = new SagaExecution(sagaLog, threadPool, saga, adapterLoader);

        SagaHandoffControl handoffControl = startSagaExecutionWithThrottling(sagaExecution, input, executionId, sagaLog, sagaCommandsEnabled ? commandsByNodeId : Collections.emptyMap());

        sagasObserver.registerSaga(handoffControl);

        SelectableFuture<SagaHandoffResult> completionFuture = handoffControl.getCompletionFuture();
        SelectableFuture<SagaHandoffResult> handoffFuture = handoffControl.getHandoffFuture();

        SelectableFuture<SagaHandoffResult> future = sync ?
                completionFuture : // full saga-execution
                handoffFuture;     // first saga-log write

        completionFuture.handle((v, t) -> {
            executionIdBySagaLogId.remove(sagaLog.id());
            sagaLogPool.release(sagaLog.id());
            return null;
        });

        return future;
    }

    SagaLog acquireCleanSagaLog(Consumer<SagaExecutionTraversalContext> forwardRecoveryPreAction, Consumer<SagaExecutionTraversalContext> forwardRecoveryPostAction) {
        return recursiveAcquireCleanSagaLog(new LinkedHashSet<>(), forwardRecoveryPreAction, forwardRecoveryPostAction);
    }

    private SagaLog recursiveAcquireCleanSagaLog(Set<SagaLogId> attemptedSagaLogIds, Consumer<SagaExecutionTraversalContext> forwardRecoveryPreAction, Consumer<SagaExecutionTraversalContext> forwardRecoveryPostAction) {
        SagaLog sagaLog;
        try {
            sagaLog = sagaLogPool.tryAcquire(new SagaLogOwner("Thread::" + Thread.currentThread().getName()), 10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        if (sagaLog == null) {
            throw new RuntimeException("Timeout. No available saga-logs, please check configuration.");
        }
        if (deadLetterSagaPattern.matcher(sagaLog.id().getLogName()).matches()) {
            throw new RuntimeException("Dead-letter-saga acquired unintentionally");
        }
        if (!sagaLog.readIncompleteSagas().anyMatch(e -> true)) {
            return sagaLog; // sagaLog is empty, all is well
        }

        if (!attemptedSagaLogIds.add(sagaLog.id())) {
            throw new RuntimeException(String.format("Unable to acquire clean saga-log. This is the second time saga-log-id %s was chosen.", sagaLog.id()));
        }

        // sagaLog contains at least one element
        asyncAttemptForwardRecoveryOrElseMoveToDeadLetter(sagaLog, forwardRecoveryPreAction, forwardRecoveryPostAction); // will release sagaLog when done

        return recursiveAcquireCleanSagaLog(attemptedSagaLogIds, forwardRecoveryPreAction, forwardRecoveryPostAction);
    }

    CompletableFuture<Void> asyncAttemptForwardRecoveryOrElseMoveToDeadLetter(SagaLog sagaLog, Consumer<SagaExecutionTraversalContext> preAction, Consumer<SagaExecutionTraversalContext> postAction) {
        return CompletableFuture.runAsync(() -> {
            CompletableFuture<Void> future;
            try {
                future = doCompleteSagaLog(sagaLog, false, false, preAction, postAction);
            } catch (Throwable t) {
                future = CompletableFuture.failedFuture(t); // wrap
            }
            try {
                future.join(); // wait for completion and unwrap
            } catch (Throwable t) {
                try {
                    String msg = String.format("Failed to retry saga-executions from saga-log %s, moving all entries to dead-saga-log.", sagaLog.id());
                    LOG.warn(msg, t);
                    SagaLog deadLetterSagaLog = sagaLogPool.tryTakeOwnership(new SagaLogOwner("Thread::" + Thread.currentThread().getName()), deadSagaLogId, 30, TimeUnit.SECONDS);
                    if (deadLetterSagaLog == null) {
                        throw new RuntimeException(String.format("Timeout while attempting to take ownership of %s", deadSagaLogId));
                    }
                    try {
                        moveAllEntries(sagaLog, deadLetterSagaLog).join();
                        LOG.info("Moved all saga-log-entries from {} to {}", sagaLog.id(), deadSagaLogId);
                    } finally {
                        sagaLogPool.releaseOwnership(deadSagaLogId);
                    }
                } catch (Throwable tx) {
                    LOG.error(String.format("Error while attempting to move saga-log-entries from %s to dead-saga", sagaLog.id()), tx);
                }
            } finally {
                sagaLogPool.release(sagaLog.id());
            }
        }, recoveryThreadPool);
    }

    CompletableFuture<Void> moveAllEntries(SagaLog from, SagaLog to) {
        // saga-log is not clean, move incomplete saga to recovery-log
        List<CompletableFuture<SagaLogEntry>> futures = new LinkedList<>();
        from.readIncompleteSagas().forEachOrdered(entry -> {
            SagaLogEntryBuilder deadLetterEntryBuilder = to.builder()
                    .sagaName(entry.getSagaName())
                    .entryType(entry.getEntryType())
                    .nodeId(entry.getNodeId())
                    .executionId(entry.getExecutionId())
                    .jsonData(entry.getJsonData());
            futures.add(to.write(deadLetterEntryBuilder));
        });
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).thenCompose(v -> from.truncate());
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
                    },
                    sagaExecutionTraversalContext -> {
                        List<SagaCommand> commands = commandsByNodeId.get(sagaExecutionTraversalContext.getNode().id);
                        if (commands == null) {
                            return;
                        }
                        for (SagaCommand command : commands) {
                            String cmd = command.getCommand();
                            if ("failBefore".equalsIgnoreCase(cmd)) {
                                throw new RuntimeException(String.format("failBefore saga command. nodeId: %s, sagalog-id: %s, executionId: %s", sagaExecutionTraversalContext.getNode().id, sagaLog.id(), executionId));
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
                                throw new RuntimeException(String.format("failAfter saga command. nodeId: %s, sagalog-id: %s, executionId: %s", sagaExecutionTraversalContext.getNode().id, sagaLog.id(), executionId));
                            }
                        }
                    }
            );
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

    public CompletableFuture<Void> completeLocalIncompleteSagas(ExecutorService executorService) {
        Set<SagaLogId> logIds = new LinkedHashSet<>(sagaLogPool.instanceLocalLogIds());

        return recoverIncompleteSagas(executorService, logIds, Collections.emptySet());
    }

    public CompletableFuture<Void> completeClusterWideIncompleteSagas(ExecutorService executorService) {
        Set<SagaLogId> logIds = new LinkedHashSet<>(sagaLogPool.clusterWideLogIds());
        logIds.removeIf(logId -> deadLetterSagaPattern.matcher(logId.getLogName()).matches());
        Set<SagaLogId> instanceLocalLogIds = sagaLogPool.instanceLocalLogIds();
        LinkedHashSet<SagaLogId> nonLocalClusterSagaLogs = new LinkedHashSet<>(logIds);
        nonLocalClusterSagaLogs.removeAll(instanceLocalLogIds);

        return recoverIncompleteSagas(executorService, logIds, nonLocalClusterSagaLogs);
    }

    private CompletableFuture<Void> recoverIncompleteSagas(ExecutorService executorService, Set<SagaLogId> logIds, Set<SagaLogId> nonLocalClusterSagaLogs) {
        List<CompletableFuture<CompletableFuture<Void>>> tasks = new ArrayList<>();
        for (SagaLogId logId : logIds) {
            try {
                tasks.add(CompletableFuture.supplyAsync(() -> {
                    boolean nonClusterLocalSagaLog = nonLocalClusterSagaLogs.contains(logId);
                    SagaLog sagaLog;
                    try {
                        sagaLog = sagaLogPool.tryTakeOwnership(new SagaLogOwner("Thread::" + Thread.currentThread().getName()), logId);
                        if (sagaLog == null) {
                            return CompletableFuture.completedFuture(null);
                            // unable to take ownership
                        }
                    } catch (SagaLogBusyException | SagaLogAlreadyAquiredByOtherOwnerException e) {
                        if (nonClusterLocalSagaLog) {
                            sagaLogPool.remove(logId);
                        }
                        return CompletableFuture.completedFuture(null);
                    }
                    try {
                        return doCompleteSagaLog(sagaLog, true, nonClusterLocalSagaLog, c -> {
                        }, c -> {
                        });
                    } catch (Throwable t) {
                        LOG.warn(String.format("Error completing log %s", logId), t);
                        if (nonClusterLocalSagaLog) {
                            sagaLogPool.remove(logId);
                        }
                        sagaLogPool.release(logId);
                        return CompletableFuture.failedFuture(t);
                    }
                }, executorService));
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

    CompletableFuture<Void> doCompleteSagaLog(SagaLog sagaLog, boolean releaseSagaLogWhenDone, boolean removeFromPoolWhenDone, Consumer<SagaExecutionTraversalContext> preAction, Consumer<SagaExecutionTraversalContext> postAction) {
        SagaLogId logId = sagaLog.id();
        Map<String, List<SagaLogEntry>> entriesByExecutionId = sagaLog.readIncompleteSagas().collect(groupingBy(SagaLogEntry::getExecutionId));
        if (entriesByExecutionId.isEmpty()) {
            if (removeFromPoolWhenDone) {
                sagaLogPool.remove(logId);
            }
            if (releaseSagaLogWhenDone) {
                sagaLogPool.release(logId);
            }
            return CompletableFuture.completedFuture(null);
        }
        List<CompletableFuture<Void>> futureList = new LinkedList<>();
        for (Map.Entry<String, List<SagaLogEntry>> entry : entriesByExecutionId.entrySet()) {
            String executionId = entry.getKey();
            List<SagaLogEntry> entries = entry.getValue();
            Map<String, List<SagaLogEntry>> entriesByNodeId = entries.stream().collect(groupingBy(SagaLogEntry::getNodeId));
            try {
                futureList.add(startSagaForwardRecovery(executionId, entriesByNodeId, sagaLog, preAction, postAction));
            } catch (Throwable t) {
                if (removeFromPoolWhenDone) {
                    sagaLogPool.remove(logId);
                }
                if (releaseSagaLogWhenDone) {
                    sagaLogPool.release(logId);
                }
                return CompletableFuture.failedFuture(t);
            }
        }
        return CompletableFuture.allOf(futureList.toArray(new CompletableFuture[futureList.size()]))
                .handle((v, t) -> {
                    if (t != null) {
                        if (removeFromPoolWhenDone) {
                            sagaLogPool.remove(logId);
                        }
                        if (releaseSagaLogWhenDone) {
                            sagaLogPool.release(logId);
                        }

                        throw new RuntimeException("Unable to complete saga forward recovery", t);
                    }
                    sagaLog.truncate().join();
                    if (removeFromPoolWhenDone) {
                        sagaLogPool.remove(logId);
                        sagaLogPool.delete(logId); // delete external resource associated with saga-log after truncation
                    }
                    if (releaseSagaLogWhenDone) {
                        sagaLogPool.release(logId);
                    }
                    return v;
                });
    }

    private CompletableFuture<Void> startSagaForwardRecovery(String executionId, Map<String, List<SagaLogEntry>> entriesByNodeId, SagaLog sagaLog, Consumer<SagaExecutionTraversalContext> preAction, Consumer<SagaExecutionTraversalContext> postAction) {
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
        SagaHandoffControl handoffControl = sagaExecution.executeSaga(executionId, sagaInput, true, r -> future.complete(r),
                preAction, postAction
        );
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

    public ExecutorService getRecoveryThreadPool() {
        return recoveryThreadPool;
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
