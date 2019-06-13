package no.ssb.lds.core.saga;

import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class SagaRecoveryTrigger {

    private final SagaExecutionCoordinator sec;
    private final int intervalMinSec;
    private final int intervalMaxSec;
    private final AtomicBoolean stopped = new AtomicBoolean(false);

    private final int MAX_LEGAL_INTERVAL = 60 * 60 * 24;
    private final Random random = new Random();

    private final static AtomicInteger executorThreadId = new AtomicInteger();
    private final ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1, runnable -> new Thread(runnable, "saga-recovery-" + executorThreadId.incrementAndGet()));

    public SagaRecoveryTrigger(SagaExecutionCoordinator sec, int intervalMinSec, int intervalMaxSec) {
        if (intervalMinSec < 1) {
            throw new IllegalArgumentException("illegal configuration: intervalMinSec < 1");
        }
        if (intervalMinSec > MAX_LEGAL_INTERVAL) {
            throw new IllegalArgumentException("illegal configuration: intervalMinSec > " + MAX_LEGAL_INTERVAL);
        }
        if (intervalMaxSec < 1) {
            throw new IllegalArgumentException("illegal configuration: intervalMaxSec < 1");
        }
        if (intervalMaxSec > MAX_LEGAL_INTERVAL) {
            throw new IllegalArgumentException("illegal configuration: intervalMaxSec > " + MAX_LEGAL_INTERVAL);
        }
        if (intervalMaxSec < intervalMinSec) {
            throw new IllegalArgumentException("illegal configuration: intervalMaxSec < intervalMinSec");
        }
        this.sec = sec;
        this.intervalMinSec = intervalMinSec;
        this.intervalMaxSec = intervalMaxSec;
    }

    public void start() {
        scheduledExecutorService.schedule(triggerRecoveryTask(), randomizedWaitSec(), TimeUnit.SECONDS);
    }

    private Runnable triggerRecoveryTask() {
        return () -> {
            sec.completeClusterWideIncompleteSagas();
            if (stopped.get()) {
                return;
            }
            scheduledExecutorService.schedule(triggerRecoveryTask(), randomizedWaitSec(), TimeUnit.SECONDS);
        };
    }

    private long randomizedWaitSec() {
        return intervalMinSec + random.nextInt(1 + intervalMaxSec - intervalMinSec);
    }

    public void stop() {
        stopped.set(true);
    }
}
