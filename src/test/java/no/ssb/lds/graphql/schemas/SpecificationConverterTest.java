package no.ssb.lds.graphql.schemas;

import graphql.schema.idl.TypeDefinitionRegistry;
import no.ssb.lds.core.specification.JsonSchemaBasedSpecification;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class SpecificationConverterTest {

    private SpecificationConverter converter;

    @BeforeMethod
    public void setUp() {
        converter = new SpecificationConverter();
    }

    @Test
    public void testLinks() {
    }

    @Test
    public void testLink() {
    }

    @Test
    public void testObject() {

        JsonSchemaBasedSpecification specification = JsonSchemaBasedSpecification.create(
                "/home/hadrien/Projects/SSB/gsim-raml-schema/jsonschemas/"
        );

        TypeDefinitionRegistry registry = converter.convert(specification);

        assertThat(registry).isNotNull();

    }
}