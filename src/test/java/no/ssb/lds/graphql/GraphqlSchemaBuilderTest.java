package no.ssb.lds.graphql;

import graphql.schema.GraphQLSchema;
import graphql.schema.idl.SchemaPrinter;
import no.ssb.lds.core.specification.JsonSchemaBasedSpecification;
import no.ssb.lds.core.specification.Specification;
import org.testng.annotations.Test;

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

        no.ssb.lds.graphql.GraphqlSchemaBuilder builder = new GraphqlSchemaBuilder(specification, null);
        GraphQLSchema schema = builder.getSchema();

        SchemaPrinter printer = new SchemaPrinter();
        System.err.println(printer.print(schema));

        //GraphQL gql = GraphQL.newGraphQL(schema).build();
        //ExecutionResult result = gql.execute("{ contact(id: \"test\") { name } }");
        //List<GraphQLError> errors = result.getErrors();
        //System.err.println(errors);

    }
}