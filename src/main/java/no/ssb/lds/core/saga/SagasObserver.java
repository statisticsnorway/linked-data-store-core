package no.ssb.lds.core.saga;

import no.ssb.concurrent.futureselector.FutureSelector;
import no.ssb.concurrent.futureselector.SelectableFuture;
import no.ssb.concurrent.futureselector.Selection;
import no.ssb.saga.execution.SagaHandoffControl;
import no.ssb.saga.execution.SagaHandoffResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;

public class SagasObserver implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(SagasObserver.class);

    final Thread sagaObserverThread;
    final FutureSelector<SagaHandoffResult, SagaHandoffControl> selector = new FutureSelector<>();
    final SelectableFuture<SagaHandoffResult> shutdownFuture = new SelectableFuture<>(() -> null);
    final SagaRepository sagaRepository;

    public SagasObserver(SagaRepository sagaRepository) {
        this.sagaRepository = sagaRepository;
        // ensure that selector always has at least one never-ending pending future
        selector.add(shutdownFuture, null);
        sagaObserverThread = new Thread(this, "saga-observer");
    }

    public SagasObserver start() {
        sagaObserverThread.start();
        return this;
    }

    void registerSaga(SagaHandoffControl sagaHandoffControl) {
        selector.add(sagaHandoffControl.getCompletionFuture(), sagaHandoffControl);
    }

    public SagasObserver shutdown() {
        shutdownFuture.complete(null);
        try {
            sagaObserverThread.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        return this;
    }

    private void fireSagaSuccess(SagaHandoffControl sagaHandoffControl) {
        LOG.trace(String.format("Observed Saga success. Saga-name: \"%s\", executionId: \"%s\"",
                sagaHandoffControl.getSaga().name,
                sagaHandoffControl.getExecutionId())
        );
    }

    private void fireSagaFailed(SagaHandoffControl sagaHandoffControl, Throwable t) {
        LOG.error(
                String.format("Observed Saga failure during execution. Saga-name: \"%s\", executionId: \"%s\"",
                        sagaHandoffControl.getSaga().name,
                        sagaHandoffControl.getExecutionId()),
                t
        );
    }

    @Override
    public void run() {
        try {
            while (selector.pending()) {

                // wait for next task to complete or fail
                Selection<SagaHandoffResult, SagaHandoffControl> selected = selector.select();

                /*
                 * Deal with successful completion and failure of a the selected saga
                 */
                boolean interrupted;
                do {
                    interrupted = false;
                    try {
                        selected.future.get(); // should never block
                        if (selected.control != null) {
                            fireSagaSuccess(selected.control);
                        }
                    } catch (ExecutionException e) {
                        fireSagaFailed(selected.control, e.getCause());
                    } catch (InterruptedException e) {
                        interrupted = true;
                        LOG.warn("SagaObserver thread interrupted, resetting interrupt status", e);
                        Thread.interrupted(); // clear interrupt status and continue
                    }
                } while (interrupted);
            }
            LOG.info("controlled shut-down of saga-observer.");
        } catch (Throwable t) {
            LOG.error("", t);
        }
    }
}
