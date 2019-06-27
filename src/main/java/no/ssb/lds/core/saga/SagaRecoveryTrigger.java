package no.ssb.lds.core.saga;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class SagaRecoveryTrigger {

    private static final Logger LOG = LoggerFactory.getLogger(SagaRecoveryTrigger.class);

    private final SagaExecutionCoordinator sec;
    private final int intervalMinSec;
    private final int intervalMaxSec;
    private final AtomicBoolean stopped = new AtomicBoolean(false);

    private final int MAX_LEGAL_INTERVAL_SEC = 60 * 60 * 24;
    private final Random random = new Random();

    private final static AtomicInteger triggerThreadId = new AtomicInteger();
    private final ScheduledExecutorService recoveryTriggerSingleThreadedPool = Executors.newScheduledThreadPool(1, runnable -> new Thread(runnable, "saga-recovery-trigger-" + triggerThreadId.incrementAndGet()));
    private final ExecutorService recoveryThreadPool;

    public SagaRecoveryTrigger(SagaExecutionCoordinator sec, int intervalMinSec, int intervalMaxSec, ScheduledExecutorService recoveryThreadPool) {
        if (intervalMinSec < 1) {
            throw new IllegalArgumentException("illegal configuration: intervalMinSec < 1");
        }
        if (intervalMinSec > MAX_LEGAL_INTERVAL_SEC) {
            throw new IllegalArgumentException("illegal configuration: intervalMinSec > " + MAX_LEGAL_INTERVAL_SEC);
        }
        if (intervalMaxSec < 1) {
            throw new IllegalArgumentException("illegal configuration: intervalMaxSec < 1");
        }
        if (intervalMaxSec > MAX_LEGAL_INTERVAL_SEC) {
            throw new IllegalArgumentException("illegal configuration: intervalMaxSec > " + MAX_LEGAL_INTERVAL_SEC);
        }
        if (intervalMaxSec < intervalMinSec) {
            throw new IllegalArgumentException("illegal configuration: intervalMaxSec < intervalMinSec");
        }
        this.sec = sec;
        this.intervalMinSec = intervalMinSec;
        this.intervalMaxSec = intervalMaxSec;
        this.recoveryThreadPool = recoveryThreadPool;
    }

    public void start() {
        recoveryTriggerSingleThreadedPool.schedule(triggerRecoveryTask(), randomizedWaitSec(), TimeUnit.SECONDS);
    }

    private Runnable triggerRecoveryTask() {
        return () -> {
            try {
                if (stopped.get()) {
                    LOG.info("Saga recovery trigger stopped");
                    return;
                }
                LOG.debug("Saga recovery task triggered, attempting to recover all incomplete sagas cluster-wide");
                sec.completeClusterWideIncompleteSagas(recoveryThreadPool).join();
                LOG.debug("Saga recovery task complete.");
            } catch (Throwable t) {
                LOG.error("Error while attempting to run cluster-wide saga recovery", t);
            }
            recoveryTriggerSingleThreadedPool.schedule(triggerRecoveryTask(), randomizedWaitSec(), TimeUnit.SECONDS);
        };
    }

    private long randomizedWaitSec() {
        return intervalMinSec + random.nextInt(1 + intervalMaxSec - intervalMinSec);
    }

    public void stop() {
        stopped.set(true);
    }
}
