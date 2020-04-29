package no.ssb.lds.graphql.schemas;

import graphql.schema.GraphQLSchema;
import graphql.schema.idl.SchemaParser;
import graphql.schema.idl.TypeDefinitionRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;

public class GraphQLSchemaBuilderTest {

    private static final Logger log = LoggerFactory.getLogger(GraphQLSchemaBuilderTest.class);

    /**
     * Use this to print a graphql schema out of a directory of json files.
     */
    public static void main(String... argv) {

        Boolean graphQl = false;
        Deque<String> arguments = new ArrayDeque<>(Arrays.asList(argv));
        String current = arguments.peek();
        while (!arguments.isEmpty()) {
            if ("--graphql".equals(current)) {
                graphQl = true;
            }
            current = arguments.pop();
        }

        GraphQLSchemaBuilder schemaBuilder = new GraphQLSchemaBuilder("namespace", new EmptyPersistence(), null);

        GraphQLSchema schema;
        if (graphQl) {
            log.info("Parsing graphql schema {}", current);
            TypeDefinitionRegistry definitionRegistry = new SchemaParser().parse(new File(current));
            schema = schemaBuilder.parseSchema(definitionRegistry);
        } else {
            log.error("Usage: --graphql file");
            return;
        }
        GraphQLSchema graphQL = schemaBuilder.getGraphQL(schema);
    }
}