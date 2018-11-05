package no.ssb.lds.graphql;

import no.ssb.lds.core.specification.JsonSchemaBasedSpecification;
import no.ssb.lds.core.specification.Specification;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

public class GraphqlSchemaBuilderTest {


    @Test
    public void testSchema() {

        Specification specification = JsonSchemaBasedSpecification.create(
                "spec/schemas/contact.json",
                "spec/schemas/provisionagreement.json"
        );

        no.ssb.lds.graphql.GraphqlSchemaBuilder builder = new GraphqlSchemaBuilder(specification);
        builder.getSchema();

    }
}