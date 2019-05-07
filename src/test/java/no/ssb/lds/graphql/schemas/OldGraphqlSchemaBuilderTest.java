package no.ssb.lds.graphql.schemas;

import graphql.schema.GraphQLSchema;
import graphql.schema.idl.SchemaPrinter;
import no.ssb.config.DynamicConfiguration;
import no.ssb.config.StoreBasedDynamicConfiguration;
import no.ssb.lds.api.persistence.reactivex.RxJsonPersistence;
import no.ssb.lds.api.search.SearchIndex;
import no.ssb.lds.api.specification.Specification;
import no.ssb.lds.core.search.SearchIndexConfigurator;
import no.ssb.lds.core.specification.JsonSchemaBasedSpecification;
import org.testng.annotations.Test;

public class OldGraphqlSchemaBuilderTest {

    @Test
    public void testSchemaConversion() {

        Specification specification = JsonSchemaBasedSpecification.create(
                "no/ssb/lds/graphql/schemas/examples/Agent.json",
                "no/ssb/lds/graphql/schemas/examples/AgentInRole.json",

                "no/ssb/lds/graphql/schemas/examples/AttributeComponent.json",
                "no/ssb/lds/graphql/schemas/examples/RepresentedVariable.json",
                "no/ssb/lds/graphql/schemas/examples/Variable.json",
                "no/ssb/lds/graphql/schemas/examples/UnitType.json",
                "no/ssb/lds/graphql/schemas/examples/SubstantiveValueDomain.json",
                "no/ssb/lds/graphql/schemas/examples/ComponentRelationship.json",
                "no/ssb/lds/graphql/schemas/examples/DataResource.json",
                "no/ssb/lds/graphql/schemas/examples/DescribedValueDomain.json",
                "no/ssb/lds/graphql/schemas/examples/DimentionalDataSet.json",
                "no/ssb/lds/graphql/schemas/examples/DimentionalDataStructure.json",
                "no/ssb/lds/graphql/schemas/examples/EnumeratedValueDomain.json",
                "no/ssb/lds/graphql/schemas/examples/ExchangeChannel.json",
                "no/ssb/lds/graphql/schemas/examples/IdentifierComponent.json",
                "no/ssb/lds/graphql/schemas/examples/InformationProvider.json",
                "no/ssb/lds/graphql/schemas/examples/InstanceVariable.json",
                "no/ssb/lds/graphql/schemas/examples/LogicalRecord.json",
                "no/ssb/lds/graphql/schemas/examples/MeasureComponent.json",
                "no/ssb/lds/graphql/schemas/examples/Population.json",
                "no/ssb/lds/graphql/schemas/examples/Protocol.json",
                "no/ssb/lds/graphql/schemas/examples/ProvisionAgreement.json",
                "no/ssb/lds/graphql/schemas/examples/RecordRelationship.json",

                "no/ssb/lds/graphql/schemas/examples/SentinelValueDomain.json",
                "no/ssb/lds/graphql/schemas/examples/UnitDataSet.json",
                "no/ssb/lds/graphql/schemas/examples/UnitDataStructure.json",

                "no/ssb/lds/graphql/schemas/examples/Role.json"

        );
        RxJsonPersistence fakePersistence = new EmptyPersistence();
        DynamicConfiguration configuration = new StoreBasedDynamicConfiguration.Builder()
                .values("graphql.search.enabled", "true")
                .values("search.index.provider", "testSearchIndex")
                .build();
        SearchIndex searchIndex = SearchIndexConfigurator.configureSearchIndex(configuration);
        OldGraphqlSchemaBuilder builder = new OldGraphqlSchemaBuilder(specification, fakePersistence, searchIndex,
                "/ns");
        GraphQLSchema schema = builder.getSchema();

        SchemaPrinter printer = new SchemaPrinter();
        System.err.println(printer.print(schema));

    }

}