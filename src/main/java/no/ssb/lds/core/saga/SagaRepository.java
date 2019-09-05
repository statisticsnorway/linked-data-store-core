package no.ssb.lds.core.saga;

import no.ssb.lds.api.persistence.reactivex.RxJsonPersistence;
import no.ssb.lds.api.search.SearchIndex;
import no.ssb.lds.api.specification.Specification;
import no.ssb.lds.core.persistence.PersistenceCreateOrOverwriteSagaAdapter;
import no.ssb.lds.core.persistence.PersistenceDeleteSagaAdapter;
import no.ssb.lds.core.search.DeleteIndexSagaAdapter;
import no.ssb.lds.core.search.UpdateIndexSagaAdapter;
import no.ssb.saga.api.Saga;
import no.ssb.saga.execution.adapter.AdapterLoader;

import java.util.LinkedHashMap;
import java.util.Map;

public class SagaRepository {

    public static final String SAGA_CREATE_OR_UPDATE_MANAGED_RESOURCE = "Create or update managed resource";
    public static final String SAGA_DELETE_MANAGED_RESOURCE = "Delete managed resource";

    final Map<String, Saga> sagaByName = new LinkedHashMap<>();

    final AdapterLoader adapterLoader;

    private SagaRepository(Specification specification, RxJsonPersistence persistence, SearchIndex indexer) {
        Saga.SagaBuilder createSagaBuilder = Saga.start(SAGA_CREATE_OR_UPDATE_MANAGED_RESOURCE)
                .linkTo("persistence", "search-index-update");
        createSagaBuilder.id("persistence").adapter(PersistenceCreateOrOverwriteSagaAdapter.NAME).linkToEnd();
        if (indexer != null) {
            createSagaBuilder.id("search-index-update").adapter(UpdateIndexSagaAdapter.NAME).linkToEnd();
        }
        register(createSagaBuilder.end());

        Saga.SagaBuilder deleteSagaBuilder = Saga.start(SAGA_DELETE_MANAGED_RESOURCE)
                .linkTo("persistence", "search-index-delete");
        deleteSagaBuilder.id("persistence").adapter(PersistenceDeleteSagaAdapter.NAME).linkToEnd();
        if (indexer != null) {
            deleteSagaBuilder.id("search-index-delete").adapter(DeleteIndexSagaAdapter.NAME).linkToEnd();
        }
        register(deleteSagaBuilder.end());

        this.adapterLoader = new AdapterLoader();

        adapterLoader.register(new PersistenceCreateOrOverwriteSagaAdapter(persistence, specification));
        adapterLoader.register(new PersistenceDeleteSagaAdapter(persistence));
        if (indexer != null) {
            adapterLoader.register(new UpdateIndexSagaAdapter(indexer, specification));
            adapterLoader.register(new DeleteIndexSagaAdapter(indexer, specification));
        }
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

        public SagaRepository build() {
            return new SagaRepository(specification, persistence, indexer);
        }
    }
}
