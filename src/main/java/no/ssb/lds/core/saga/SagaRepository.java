package no.ssb.lds.core.saga;

import no.ssb.lds.api.persistence.reactivex.RxJsonPersistence;
import no.ssb.lds.api.specification.Specification;
import no.ssb.lds.core.notification.KafkaCreateOrOverwriteSagaAdapter;
import no.ssb.lds.core.persistence.PersistenceCreateOrOverwriteSagaAdapter;
import no.ssb.lds.core.persistence.PersistenceDeleteSagaAdapter;
import no.ssb.saga.api.Saga;
import no.ssb.saga.execution.adapter.AdapterLoader;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.LinkedHashMap;
import java.util.Map;

public class SagaRepository {

    public static final String SAGA_CREATE_OR_UPDATE_MANAGED_RESOURCE = "Create or update managed resource";
    public static final String SAGA_DELETE_MANAGED_RESOURCE = "Delete managed resource";

    final Map<String, Saga> sagaByName = new LinkedHashMap<>();

    final AdapterLoader adapterLoader;

    public SagaRepository(Specification specification, RxJsonPersistence persistence) {
        register(Saga
                .start(SAGA_CREATE_OR_UPDATE_MANAGED_RESOURCE).linkTo("persistence")
                .id("persistence").adapter(PersistenceCreateOrOverwriteSagaAdapter.NAME).linkToEnd()
                .end());
        register(Saga.start(SAGA_DELETE_MANAGED_RESOURCE).linkTo("persistence")
                .id("persistence").adapter(PersistenceDeleteSagaAdapter.NAME).linkToEnd()
                .end());

        adapterLoader = new AdapterLoader()
                .register(new PersistenceCreateOrOverwriteSagaAdapter(persistence, specification))
                .register(new PersistenceDeleteSagaAdapter(persistence));
    }

    public SagaRepository(Specification specification, RxJsonPersistence persistence, KafkaProducer producer) {
        register(Saga
                .start(SAGA_CREATE_OR_UPDATE_MANAGED_RESOURCE).linkTo("persistence")
                .id("persistence").adapter(PersistenceCreateOrOverwriteSagaAdapter.NAME).linkTo("notification")
                .id("notification").adapter(KafkaCreateOrOverwriteSagaAdapter.NAME).linkToEnd()
                .end());
        register(Saga.start(SAGA_DELETE_MANAGED_RESOURCE).linkTo("persistence")
                .id("persistence").adapter(PersistenceDeleteSagaAdapter.NAME).linkToEnd()
                .end());

        adapterLoader = new AdapterLoader()
                .register(new PersistenceCreateOrOverwriteSagaAdapter(persistence, specification))
                .register(new KafkaCreateOrOverwriteSagaAdapter(producer, specification))
                .register(new PersistenceDeleteSagaAdapter(persistence));
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
