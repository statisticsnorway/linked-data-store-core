package no.ssb.lds.graphql.jsonSchema;

import graphql.Assert;
import graphql.schema.GraphQLSchema;
import graphql.schema.idl.SchemaParser;
import graphql.schema.idl.TypeDefinitionRegistry;
import no.ssb.lds.graphql.schemas.GraphQLSchemaBuilder;
import org.json.JSONObject;
import org.testng.annotations.Test;

import java.io.File;
import java.net.URL;
import java.util.LinkedHashMap;
import java.util.Optional;

public class GraphQLToJsonConverterTest {

    @Test
    public void thatConversionWorks() {
        File graphQLFile = new File("src/test/resources/spec/abstract/graphqlschemas/schema.graphql");

        TypeDefinitionRegistry definitionRegistry = parseSchemaFile(graphQLFile);

        GraphQLSchema schema = GraphQLSchemaBuilder.parseSchema(definitionRegistry);
        GraphQLToJsonConverter graphQLToJsonConverter = new GraphQLToJsonConverter(schema);
        LinkedHashMap<String, JSONObject> jsonMap = graphQLToJsonConverter.createSpecification(schema);

        jsonMap.forEach((name, json) -> {
            System.out.printf("%s :: %s%n", name, json);
        });

        Assert.assertNotEmpty(jsonMap.values());
    }

    private static TypeDefinitionRegistry parseSchemaFile(File graphQLFile) {
        TypeDefinitionRegistry definitionRegistry;
        URL systemResource = ClassLoader.getSystemResource(graphQLFile.getPath());

        if (Optional.ofNullable(systemResource).isPresent()) {
            definitionRegistry = new SchemaParser().parse(new File(systemResource.getPath()));
        } else {
            definitionRegistry = new SchemaParser().parse(new File(graphQLFile.getPath()));
        }
        return definitionRegistry;
    }
}