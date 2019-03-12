package no.ssb.lds.graphql.schemas;

import graphql.schema.GraphQLSchema;
import graphql.schema.idl.SchemaPrinter;
import io.reactivex.Completable;
import io.reactivex.Flowable;
import io.reactivex.Maybe;
import io.reactivex.Single;
import no.ssb.config.DynamicConfiguration;
import no.ssb.config.StoreBasedDynamicConfiguration;
import no.ssb.lds.api.json.JsonNavigationPath;
import no.ssb.lds.api.persistence.PersistenceDeletePolicy;
import no.ssb.lds.api.persistence.PersistenceException;
import no.ssb.lds.api.persistence.Transaction;
import no.ssb.lds.api.persistence.json.JsonDocument;
import no.ssb.lds.api.persistence.reactivex.Range;
import no.ssb.lds.api.persistence.reactivex.RxJsonPersistence;
import no.ssb.lds.api.specification.Specification;
import no.ssb.lds.core.extension.SearchIndex;
import no.ssb.lds.core.extension.SearchIndexConfigurator;
import no.ssb.lds.core.specification.JsonSchemaBasedSpecification;
import org.testng.annotations.Test;

import java.time.ZonedDateTime;

public class GraphqlSchemaBuilderTest {

    @Test
    public void testSchemaConversion() {

        Specification specification = JsonSchemaBasedSpecification.create(
                "no/ssb/lds/graphql/schemas/Agent.json",
                "no/ssb/lds/graphql/schemas/AgentInRole.json",

                "no/ssb/lds/graphql/schemas/AttributeComponent.json",
                "no/ssb/lds/graphql/schemas/RepresentedVariable.json",
                "no/ssb/lds/graphql/schemas/Variable.json",
                "no/ssb/lds/graphql/schemas/UnitType.json",
                "no/ssb/lds/graphql/schemas/SubstantiveValueDomain.json",
                "no/ssb/lds/graphql/schemas/ComponentRelationship.json",
                "no/ssb/lds/graphql/schemas/DataResource.json",
                "no/ssb/lds/graphql/schemas/DescribedValueDomain.json",
                "no/ssb/lds/graphql/schemas/DimentionalDataSet.json",
                "no/ssb/lds/graphql/schemas/DimentionalDataStructure.json",
                "no/ssb/lds/graphql/schemas/EnumeratedValueDomain.json",
                "no/ssb/lds/graphql/schemas/ExchangeChannel.json",
                "no/ssb/lds/graphql/schemas/IdentifierComponent.json",
                "no/ssb/lds/graphql/schemas/InformationProvider.json",
                "no/ssb/lds/graphql/schemas/InstanceVariable.json",
                "no/ssb/lds/graphql/schemas/LogicalRecord.json",
                "no/ssb/lds/graphql/schemas/MeasureComponent.json",
                "no/ssb/lds/graphql/schemas/Population.json",
                "no/ssb/lds/graphql/schemas/Protocol.json",
                "no/ssb/lds/graphql/schemas/ProvisionAgreement.json",
                "no/ssb/lds/graphql/schemas/RecordRelationship.json",

                "no/ssb/lds/graphql/schemas/SentinelValueDomain.json",
                "no/ssb/lds/graphql/schemas/UnitDataSet.json",
                "no/ssb/lds/graphql/schemas/UnitDataStructure.json",

                "no/ssb/lds/graphql/schemas/Role.json"

        );
        RxJsonPersistence fakePersistence = new MockPersistence();
        DynamicConfiguration configuration = new StoreBasedDynamicConfiguration.Builder()
                .values("search.index.provider", "testSearchIndex")
                .build();
        SearchIndex searchIndex = SearchIndexConfigurator.configureSearchIndex(configuration, specification);
        GraphqlSchemaBuilder builder = new GraphqlSchemaBuilder(specification, fakePersistence, searchIndex,
                "/ns");
        GraphQLSchema schema = builder.getSchema();

        SchemaPrinter printer = new SchemaPrinter();
        System.err.println(printer.print(schema));

    }

    private class MockPersistence implements RxJsonPersistence {
        @Override
        public Maybe<JsonDocument> readDocument(Transaction tx, ZonedDateTime snapshot, String ns, String entityName, String id) {
            return null;
        }

        @Override
        public Flowable<JsonDocument> readDocuments(Transaction tx, ZonedDateTime snapshot, String ns, String entityName, Range<String> range) {
            return null;
        }

        @Override
        public Flowable<JsonDocument> readDocumentVersions(Transaction tx, String ns, String entityName, String id, Range<ZonedDateTime> range) {
            return null;
        }

        @Override
        public Flowable<JsonDocument> readLinkedDocuments(Transaction tx, ZonedDateTime snapshot, String ns, String entityName, String id, JsonNavigationPath jsonNavigationPath, String targetEntityName, Range<String> range) {
            return null;
        }

        @Override
        public Completable createOrOverwrite(Transaction tx, JsonDocument document, Specification specification) {
            return null;
        }

        @Override
        public Completable deleteDocument(Transaction tx, String ns, String entityName, String id, ZonedDateTime version, PersistenceDeletePolicy policy) {
            return null;
        }

        @Override
        public Completable deleteAllDocumentVersions(Transaction tx, String ns, String entity, String id, PersistenceDeletePolicy policy) {
            return null;
        }

        @Override
        public Completable deleteAllEntities(Transaction tx, String namespace, String entity, Specification specification) {
            return null;
        }

        @Override
        public Completable markDocumentDeleted(Transaction transaction, String ns, String entityName, String id, ZonedDateTime version, PersistenceDeletePolicy policy) {
            return null;
        }

        @Override
        public Single<Boolean> hasPrevious(Transaction tx, ZonedDateTime snapshot, String ns, String entityName, String id) {
            return null;
        }

        @Override
        public Single<Boolean> hasNext(Transaction tx, ZonedDateTime snapshot, String ns, String entityName, String id) {
            return null;
        }

        @Override
        public Transaction createTransaction(boolean readOnly) throws PersistenceException {
            return null;
        }

        @Override
        public Flowable<JsonDocument> findDocument(Transaction tx, ZonedDateTime snapshot, String namespace, String entityName, JsonNavigationPath path, String value, Range<String> range) {
            return null;
        }

        @Override
        public void close() throws PersistenceException {

        }
    }
}