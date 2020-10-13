package no.ssb.lds.core.saga;

import no.ssb.lds.api.persistence.reactivex.RxJsonPersistence;
import no.ssb.lds.api.search.SearchIndex;
import no.ssb.lds.api.specification.Specification;
import no.ssb.lds.core.persistence.BatchSagaAdapter;
import no.ssb.lds.core.persistence.PersistenceCreateOrOverwriteSagaAdapter;
import no.ssb.lds.core.persistence.PersistenceDeleteSagaAdapter;
import no.ssb.lds.core.search.DeleteIndexSagaAdapter;
import no.ssb.lds.core.search.UpdateIndexSagaAdapter;
import no.ssb.lds.core.txlog.AppendTxLogAdapter;
import no.ssb.lds.core.txlog.DeleteTxLogAdapter;
import no.ssb.lds.core.txlog.TxlogRawdataPool;
import no.ssb.saga.api.Saga;
import no.ssb.saga.execution.adapter.AdapterLoader;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SagaRepository {

    public static final String SAGA_BATCH = "Batch";
    public static final String SAGA_BATCH_NO_TX_LOG = "Batch without writing to transaction log";
    public static final String SAGA_CREATE_OR_UPDATE_MANAGED_RESOURCE = "Create or update managed resource";
    public static final String SAGA_CREATE_OR_UPDATE_MANAGED_RESOURCE_NO_TX_LOG = "Create or update managed resource without writing to transaction log";
    public static final String SAGA_DELETE_MANAGED_RESOURCE = "Delete managed resource";
    public static final String SAGA_DELETE_MANAGED_RESOURCE_NO_TX_LOG = "Delete managed resource without writing to transaction log";

    final Map<String, Saga> sagaByName = new ConcurrentHashMap<>();

    final AdapterLoader adapterLoader;

    private SagaRepository(Specification specification, RxJsonPersistence persistence, SearchIndex indexer, TxlogRawdataPool txLogPool) {
        adapterLoader = new AdapterLoader();
        adapterLoader.register(new PersistenceCreateOrOverwriteSagaAdapter(persistence, specification));
        adapterLoader.register(new PersistenceDeleteSagaAdapter(persistence));
        adapterLoader.register(new AppendTxLogAdapter(txLogPool));
        adapterLoader.register(new DeleteTxLogAdapter(txLogPool));
        if (indexer != null) {
            adapterLoader.register(new UpdateIndexSagaAdapter(indexer, specification));
            adapterLoader.register(new DeleteIndexSagaAdapter(indexer, specification));
        }
        adapterLoader.register(new BatchSagaAdapter(persistence, specification));

        register(buildCreateOrUpdateSaga(indexer));
        register(buildCreateOrUpdateSagaWithoutTransactionLog(indexer));
        register(buildDeleteSaga(indexer));
        register(buildDeleteSagaWithoutTransactionLog(indexer));
        register(buildBatchSaga());
        register(buildBatchSagaNoTxLog());
    }

    private Saga buildBatchSaga() {
        Saga.SagaBuilder builder = Saga.start(SAGA_BATCH)
                .linkTo("txlog");
        builder.id("txlog").adapter(AppendTxLogAdapter.NAME).linkTo("persistence");
        builder.id("persistence").adapter(BatchSagaAdapter.NAME).linkToEnd();
        return builder.end();
    }

    private Saga buildBatchSagaNoTxLog() {
        Saga.SagaBuilder builder = Saga.start(SAGA_BATCH_NO_TX_LOG)
                .linkTo("persistence");
        builder.id("persistence").adapter(BatchSagaAdapter.NAME).linkToEnd();
        return builder.end();
    }

    private Saga buildCreateOrUpdateSaga(SearchIndex indexer) {
        Saga.SagaBuilder createSagaBuilder = Saga.start(SAGA_CREATE_OR_UPDATE_MANAGED_RESOURCE)
                .linkTo("txlog");
        if (indexer != null) {
            createSagaBuilder.id("txlog").adapter(AppendTxLogAdapter.NAME).linkTo("persistence", "search-index-update");
            createSagaBuilder.id("search-index-update").adapter(UpdateIndexSagaAdapter.NAME).linkToEnd();
        } else {
            createSagaBuilder.id("txlog").adapter(AppendTxLogAdapter.NAME).linkTo("persistence");
        }
        createSagaBuilder.id("persistence").adapter(PersistenceCreateOrOverwriteSagaAdapter.NAME).linkToEnd();
        return createSagaBuilder.end();
    }

    private Saga buildCreateOrUpdateSagaWithoutTransactionLog(SearchIndex indexer) {
        Saga.SagaBuilder createSagaBuilder;
        if (indexer != null) {
            createSagaBuilder = Saga.start(SAGA_CREATE_OR_UPDATE_MANAGED_RESOURCE_NO_TX_LOG)
                    .linkTo("persistence", "search-index-update");
            createSagaBuilder.id("search-index-update").adapter(UpdateIndexSagaAdapter.NAME).linkToEnd();
        } else {
            createSagaBuilder = Saga.start(SAGA_CREATE_OR_UPDATE_MANAGED_RESOURCE_NO_TX_LOG)
                    .linkTo("persistence");
        }
        createSagaBuilder.id("persistence").adapter(PersistenceCreateOrOverwriteSagaAdapter.NAME).linkToEnd();
        return createSagaBuilder.end();
    }

    private Saga buildDeleteSaga(SearchIndex indexer) {
        Saga.SagaBuilder deleteSagaBuilder = Saga.start(SAGA_DELETE_MANAGED_RESOURCE)
                .linkTo("txlog");
        if (indexer != null) {
            deleteSagaBuilder.id("txlog").adapter(DeleteTxLogAdapter.NAME).linkTo("persistence", "search-index-delete");
            deleteSagaBuilder.id("search-index-delete").adapter(DeleteIndexSagaAdapter.NAME).linkToEnd();
        } else {
            deleteSagaBuilder.id("txlog").adapter(DeleteTxLogAdapter.NAME).linkTo("persistence");
        }
        deleteSagaBuilder.id("persistence").adapter(PersistenceDeleteSagaAdapter.NAME).linkToEnd();
        return deleteSagaBuilder.end();
    }

    private Saga buildDeleteSagaWithoutTransactionLog(SearchIndex indexer) {
        Saga.SagaBuilder deleteSagaBuilder;
        if (indexer != null) {
            deleteSagaBuilder = Saga.start(SAGA_DELETE_MANAGED_RESOURCE_NO_TX_LOG)
                    .linkTo("persistence", "search-index-delete");
            deleteSagaBuilder.id("search-index-delete").adapter(DeleteIndexSagaAdapter.NAME).linkToEnd();
        } else {
            deleteSagaBuilder = Saga.start(SAGA_DELETE_MANAGED_RESOURCE_NO_TX_LOG)
                    .linkTo("persistence");
        }
        deleteSagaBuilder.id("persistence").adapter(PersistenceDeleteSagaAdapter.NAME).linkToEnd();
        return deleteSagaBuilder.end();
    }

    public AdapterLoader getAdapterLoader() {
        return adapterLoader;
    }

    SagaRepository register(Saga saga) {
        sagaByName.put(saga.name, saga);
        return this;
    }

    public Saga get(String sagaName) {
        return sagaByName.get(sagaName);
    }

    public static class Builder {

        Specification specification;
        RxJsonPersistence persistence;
        SearchIndex indexer;
        TxlogRawdataPool txlogRawdataPool;

        public Builder specification(Specification specification) {
            this.specification = specification;
            return this;
        }

        public Builder persistence(RxJsonPersistence persistence) {
            this.persistence = persistence;
            return this;
        }

        public Builder indexer(SearchIndex indexer) {
            this.indexer = indexer;
            return this;
        }

        public Builder txLogRawdataPool(TxlogRawdataPool txlogRawdataPool) {
            this.txlogRawdataPool = txlogRawdataPool;
            return this;
        }

        public SagaRepository build() {
            return new SagaRepository(specification, persistence, indexer, txlogRawdataPool);
        }
    }
}
