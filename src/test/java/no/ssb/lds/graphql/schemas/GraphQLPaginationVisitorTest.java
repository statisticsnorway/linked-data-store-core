package no.ssb.lds.graphql.schemas;

import graphql.schema.GraphQLSchema;
import graphql.schema.GraphQLType;
import graphql.schema.TypeTraverser;
import graphql.schema.idl.RuntimeWiring;
import graphql.schema.idl.SchemaGenerator;
import graphql.schema.idl.SchemaParser;
import graphql.schema.idl.SchemaPrinter;
import graphql.schema.idl.TypeDefinitionRegistry;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class GraphQLPaginationVisitorTest {

    @Test
    public void testPagination() {
        TypeDefinitionRegistry definitionRegistry = new SchemaParser().parse(
                "" +
                        "directive @link(reverseName: String, pagination: Boolean) on FIELD_DEFINITION\n" +
                        "" +
                        "type Source {" +
                        "   foo: String" +
                        "   link: [Target] @link(pagination : true, reverseName: \"sources\")" +
                        "}" +
                        "type Target {" +
                        "   bar: String" +
                        "}" +
                        "type Query {" +
                        "   Source: Source" +
                        "}"
        );
        RuntimeWiring runtimeWiring = RuntimeWiring.newRuntimeWiring().build();
        GraphQLSchema schema = new SchemaGenerator().makeExecutableSchema(definitionRegistry, runtimeWiring);

        Map<String, GraphQLType> typeMap = new HashMap<>(schema.getTypeMap());
        new TypeTraverser().depthFirst(
                new GraphQLPaginationVisitor(typeMap),
                typeMap.values()
        );

        assertSource(typeMap);
        assertSourceConnection(typeMap);
        assertSourceEdge(typeMap);
    }

    void assertSourceConnection(Map<String, GraphQLType> typeMap) {
        assertTypeDefinition(typeMap, "SourceTargetConnection", "" +
                "type SourceTargetConnection {" +
                "   edges: [SourceTargetEdge!]!" +
                "   pageInfo: PageInfo!" +
                "}");
    }

    @Test
    public void testReversePagination() {
        TypeDefinitionRegistry definitionRegistry = new SchemaParser().parse(
                "" +
                        "directive @link(reverseName: String, pagination: Boolean) on FIELD_DEFINITION " +
                        "directive @reverseLink(mappedBy: String, pagination: Boolean) on FIELD_DEFINITION " +
                        "" +
                        "type Source {" +
                        "   foo: String" +
                        "   link: [Target] @link(pagination : true, reverseName: \"sources\")" +
                        "}" +
                        "type Target {" +
                        "   bar: String" +
                        "   sources : [Source!]! @reverseLink(mappedBy: \"link\", pagination: true)" +
                        "}" +
                        "type Query {" +
                        "   Source: Source" +
                        "}"
        );
        RuntimeWiring runtimeWiring = RuntimeWiring.newRuntimeWiring().build();
        GraphQLSchema schema = new SchemaGenerator().makeExecutableSchema(definitionRegistry, runtimeWiring);

        Map<String, GraphQLType> typeMap = new HashMap<>(schema.getTypeMap());
        new TypeTraverser().depthFirst(
                new GraphQLPaginationVisitor(typeMap),
                typeMap.values()
        );

        assertSource(typeMap);
        assertSourceConnection(typeMap);
        assertSourceEdge(typeMap);

        assertTypeDefinition(typeMap, "Target", "" +
                "type Target {" +
                "  bar: String" +
                "  sources(after: String, " +
                "          before: String, " +
                "          first: Int, " +
                "          last: Int): TargetSourceConnection @reverseLink(mappedBy : \"link\", pagination : true)" +
                "}"
        );

        assertTypeDefinition(typeMap, "TargetSourceConnection", "" +
                "type TargetSourceConnection {" +
                "  edges: [TargetSourceEdge!]!" +
                "  pageInfo: PageInfo!" +
                "}"
        );

        assertTypeDefinition(typeMap, "TargetSourceEdge", "" +
                "type TargetSourceEdge {" +
                "  cursor: String!" +
                "  node: Source!" +
                "}"
        );

    }

    @Test
    public void testPaginationDisabled() {
        TypeDefinitionRegistry definitionRegistry = new SchemaParser().parse(
                "" +
                        "directive @link(reverseName: String, pagination: Boolean) on FIELD_DEFINITION " +
                        "directive @reverseLink(mappedBy: String, pagination: Boolean) on FIELD_DEFINITION " +
                        "" +
                        "type Source {" +
                        "   foo: String" +
                        "   link: [Target] @link(pagination : false, reverseName: \"sources\")" +
                        "}" +
                        "type Target {" +
                        "   bar: String" +
                        "   sources : [Source!]! @reverseLink(mappedBy: \"link\", pagination: false)" +
                        "}" +
                        "type Query {" +
                        "   Source: Source" +
                        "}"
        );
        RuntimeWiring runtimeWiring = RuntimeWiring.newRuntimeWiring().build();
        GraphQLSchema schema = new SchemaGenerator().makeExecutableSchema(definitionRegistry, runtimeWiring);

        Map<String, GraphQLType> typeMap = new HashMap<>(schema.getTypeMap());
        new TypeTraverser().depthFirst(
                new GraphQLPaginationVisitor(typeMap),
                typeMap.values()
        );

        assertTypeDefinition(typeMap, "Source", "" +
                "type Source {" +
                "  foo: String" +
                "  link: [Target] @link(pagination : false, reverseName : \"sources\")" +
                "}"
        );

        assertTypeDefinition(typeMap, "Target", "" +
                "type Target {" +
                "  bar: String" +
                "  sources: [Source!]! @reverseLink(mappedBy : \"link\", pagination : false)" +
                "}"
        );

    }

    void assertSourceEdge(Map<String, GraphQLType> typeMap) {
        assertTypeDefinition(typeMap, "SourceTargetEdge", "" +
                "type SourceTargetEdge {" +
                "  cursor: String!" +
                "  node: Target!" +
                "}");
    }

    void assertSource(Map<String, GraphQLType> typeMap) {
        assertTypeDefinition(typeMap, "Source", "" +
                "type Source {" +
                "  foo: String" +
                "  link(after: String, " +
                "       before: String, " +
                "       first: Int, " +
                "       last: Int): SourceTargetConnection @link(pagination : true, reverseName: \"sources\")" +
                "}");
    }

    void assertTypeDefinition(Map<String, GraphQLType> typeMap, String typeName, String expectedDefinition) {
        String target = new SchemaPrinter().print(typeMap.get(typeName));
        assertThat(target).isEqualToIgnoringWhitespace(expectedDefinition);
    }
}