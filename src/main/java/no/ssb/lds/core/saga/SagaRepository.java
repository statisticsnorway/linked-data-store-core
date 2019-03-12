package no.ssb.lds.core.saga;

import no.ssb.lds.api.persistence.reactivex.RxJsonPersistence;
import no.ssb.lds.api.specification.Specification;
import no.ssb.lds.core.extension.UpdateIndexSagaAdapter;
import no.ssb.lds.core.extension.DeleteIndexSagaAdapter;
import no.ssb.lds.core.extension.SearchIndex;
import no.ssb.lds.core.persistence.PersistenceCreateOrOverwriteSagaAdapter;
import no.ssb.lds.core.persistence.PersistenceDeleteSagaAdapter;
import no.ssb.saga.api.Saga;
import no.ssb.saga.execution.adapter.AdapterLoader;

import java.util.LinkedHashMap;
import java.util.Map;

public class SagaRepository {

    public static final String SAGA_CREATE_OR_UPDATE_MANAGED_RESOURCE = "Create or update managed resource";
    public static final String SAGA_DELETE_MANAGED_RESOURCE = "Delete managed resource";

    final Map<String, Saga> sagaByName = new LinkedHashMap<>();

    final AdapterLoader adapterLoader;

    public SagaRepository(Specification specification, RxJsonPersistence persistence, SearchIndex indexer) {
        register(Saga
                .start(SAGA_CREATE_OR_UPDATE_MANAGED_RESOURCE).linkTo("persistence", "search-index-update")
                .id("persistence").adapter(PersistenceCreateOrOverwriteSagaAdapter.NAME).linkToEnd()
                .id("search-index-update").adapter(UpdateIndexSagaAdapter.NAME).linkToEnd()
                .end());
        register(Saga.start(SAGA_DELETE_MANAGED_RESOURCE).linkTo("persistence", "search-index-delete")
                .id("persistence").adapter(PersistenceDeleteSagaAdapter.NAME).linkToEnd()
                .id("search-index-delete").adapter(DeleteIndexSagaAdapter.NAME).linkToEnd()
                .end());

        adapterLoader = new AdapterLoader()
                .register(new PersistenceCreateOrOverwriteSagaAdapter(persistence, specification))
                .register(new PersistenceDeleteSagaAdapter(persistence))
                .register(new UpdateIndexSagaAdapter(indexer, specification))
                .register(new DeleteIndexSagaAdapter(indexer, specification));
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
}
