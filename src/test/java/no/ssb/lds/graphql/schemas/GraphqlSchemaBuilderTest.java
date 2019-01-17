package no.ssb.lds.graphql.schemas;

import graphql.schema.GraphQLSchema;
import graphql.schema.idl.SchemaPrinter;
import no.ssb.lds.api.persistence.PersistenceDeletePolicy;
import no.ssb.lds.api.persistence.PersistenceException;
import no.ssb.lds.api.persistence.Transaction;
import no.ssb.lds.api.persistence.TransactionFactory;
import no.ssb.lds.api.persistence.json.JsonDocument;
import no.ssb.lds.api.persistence.json.JsonPersistence;
import no.ssb.lds.api.persistence.streaming.Fragment;
import no.ssb.lds.api.persistence.streaming.Persistence;
import no.ssb.lds.api.specification.Specification;
import no.ssb.lds.core.specification.JsonSchemaBasedSpecification;
import no.ssb.lds.graphql.schemas.GraphqlSchemaBuilder;
import org.testng.annotations.Test;

import java.time.ZonedDateTime;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;

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
        JsonPersistence fakePersistence = new MockPersistence();
        GraphqlSchemaBuilder builder = new GraphqlSchemaBuilder(specification, fakePersistence,
                "/ns");
        GraphQLSchema schema = builder.getSchema();

        SchemaPrinter printer = new SchemaPrinter();
        System.err.println(printer.print(schema));

    }

    private class MockPersistence implements JsonPersistence {
        @Override
        public Persistence getPersistence() {
            return new Persistence() {
                @Override
                public TransactionFactory transactionFactory() throws PersistenceException {
                    return null;
                }

                @Override
                public Transaction createTransaction(boolean readOnly) throws PersistenceException {
                    return null;
                }

                @Override
                public CompletableFuture<Void> createOrOverwrite(Transaction transaction, Flow.Publisher<Fragment> publisher) throws PersistenceException {
                    return null;
                }

                @Override
                public Flow.Publisher<Fragment> read(Transaction transaction, ZonedDateTime snapshot, String namespace, String entity, String id) throws PersistenceException {
                    return null;
                }

                @Override
                public Flow.Publisher<Fragment> readVersions(Transaction transaction, ZonedDateTime snapshotFrom, ZonedDateTime snapshotTo, String namespace, String entity, String id, ZonedDateTime firstVersion, int limit) throws PersistenceException {
                    return null;
                }

                @Override
                public Flow.Publisher<Fragment> readAllVersions(Transaction transaction, String namespace, String entity, String id, ZonedDateTime firstVersion, int limit) throws PersistenceException {
                    return null;
                }

                @Override
                public CompletableFuture<Void> delete(Transaction transaction, String namespace, String entity, String id, ZonedDateTime version, PersistenceDeletePolicy policy) throws PersistenceException {
                    return null;
                }

                @Override
                public CompletableFuture<Void> deleteAllVersions(Transaction transaction, String namespace, String entity, String id, PersistenceDeletePolicy policy) throws PersistenceException {
                    return null;
                }

                @Override
                public CompletableFuture<Void> markDeleted(Transaction transaction, String namespace, String entity, String id, ZonedDateTime version, PersistenceDeletePolicy policy) throws PersistenceException {
                    return null;
                }

                @Override
                public Flow.Publisher<Fragment> findAll(Transaction transaction, ZonedDateTime snapshot, String namespace, String entity, String firstId, int limit) throws PersistenceException {
                    return null;
                }

                @Override
                public Flow.Publisher<Fragment> find(Transaction transaction, ZonedDateTime snapshot, String namespace, String entity, String path, byte[] value, String firstId, int limit) throws PersistenceException {
                    return null;
                }

                @Override
                public void close() throws PersistenceException {

                }
            };
        }

        @Override
        public TransactionFactory transactionFactory() throws PersistenceException {
            return null;
        }

        @Override
        public Transaction createTransaction(boolean readOnly) throws PersistenceException {
            return null;
        }

        @Override
        public CompletableFuture<Void> createOrOverwrite(Transaction transaction, JsonDocument document, Specification specification) throws PersistenceException {
            return null;
        }

        @Override
        public CompletableFuture<JsonDocument> read(Transaction transaction, ZonedDateTime snapshot, String namespace, String entity, String id) throws PersistenceException {
            return null;
        }

        @Override
        public CompletableFuture<Iterable<JsonDocument>> readVersions(Transaction transaction, ZonedDateTime snapshotFrom, ZonedDateTime snapshotTo, String namespace, String entity, String id, ZonedDateTime firstVersion, int limit) throws PersistenceException {
            return null;
        }

        @Override
        public CompletableFuture<Iterable<JsonDocument>> readAllVersions(Transaction transaction, String namespace, String entity, String id, ZonedDateTime firstVersion, int limit) throws PersistenceException {
            return null;
        }

        @Override
        public CompletableFuture<Void> delete(Transaction transaction, String namespace, String entity, String id, ZonedDateTime version, PersistenceDeletePolicy policy) throws PersistenceException {
            return null;
        }

        @Override
        public CompletableFuture<Void> deleteAllVersions(Transaction transaction, String namespace, String entity, String id, PersistenceDeletePolicy policy) throws PersistenceException {
            return null;
        }

        @Override
        public CompletableFuture<Void> markDeleted(Transaction transaction, String namespace, String entity, String id, ZonedDateTime version, PersistenceDeletePolicy policy) throws PersistenceException {
            return null;
        }

        @Override
        public CompletableFuture<Iterable<JsonDocument>> findAll(Transaction transaction, ZonedDateTime snapshot, String namespace, String entity, String firstId, int limit) throws PersistenceException {
            return null;
        }

        @Override
        public CompletableFuture<Iterable<JsonDocument>> find(Transaction transaction, ZonedDateTime snapshot, String namespace, String entity, String path, Object value, String firstId, int limit) throws PersistenceException {
            return null;
        }

        @Override
        public void close() throws PersistenceException {

        }
    }
}