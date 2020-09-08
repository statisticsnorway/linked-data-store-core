package no.ssb.lds.graphql.schemas.visitors;

import graphql.schema.GraphQLNamedType;
import graphql.schema.GraphQLSchema;
import graphql.schema.SchemaTraverser;
import graphql.schema.idl.RuntimeWiring;
import graphql.schema.idl.SchemaGenerator;
import graphql.schema.idl.SchemaParser;
import graphql.schema.idl.SchemaPrinter;
import graphql.schema.idl.TypeDefinitionRegistry;
import org.assertj.core.api.Assertions;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

public class AddConnectionVisitorTest {

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

        Map<String, GraphQLNamedType> typeMap = new HashMap<>(schema.getTypeMap());
        new SchemaTraverser().depthFirst(
                new AddConnectionVisitor(typeMap),
                typeMap.values()
        );

        assertSource(typeMap);
        assertSourceConnection(typeMap);
        assertSourceEdge(typeMap);
    }

    void assertSourceConnection(Map<String, GraphQLNamedType> typeMap) {
        assertTypeDefinition(typeMap, "TargetConnection", "" +
                "type TargetConnection {" +
                "   edges: [TargetEdge!]!" +
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

        Map<String, GraphQLNamedType> typeMap = new HashMap<>(schema.getTypeMap());
        new SchemaTraverser().depthFirst(
                new AddConnectionVisitor(typeMap),
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
                "          last: Int): SourceConnection @reverseLink(mappedBy : \"link\", pagination : true)" +
                "}"
        );

        assertTypeDefinition(typeMap, "SourceConnection", "" +
                "type SourceConnection {" +
                "  edges: [SourceEdge!]!" +
                "  pageInfo: PageInfo!" +
                "}"
        );

        assertTypeDefinition(typeMap, "SourceEdge", "" +
                "type SourceEdge {" +
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

        Map<String, GraphQLNamedType> typeMap = new HashMap<>(schema.getTypeMap());
        new SchemaTraverser().depthFirst(
                new AddConnectionVisitor(typeMap),
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

    void assertSourceEdge(Map<String, GraphQLNamedType> typeMap) {
        assertTypeDefinition(typeMap, "TargetEdge", "" +
                "type TargetEdge {" +
                "  cursor: String!" +
                "  node: Target!" +
                "}");
    }

    void assertSource(Map<String, GraphQLNamedType> typeMap) {
        assertTypeDefinition(typeMap, "Source", "" +
                "type Source {" +
                "  foo: String" +
                "  link(after: String," +
                "      before: String," +
                "       first: Int," +
                "  last: Int): TargetConnection @link(pagination : true, reverseName : \"sources\")" +
                "}");
    }

    void assertTypeDefinition(Map<String, GraphQLNamedType> typeMap, String typeName, String expectedDefinition) {
        String target = new SchemaPrinter().print(typeMap.get(typeName));
        Assertions.assertThat(target).isEqualToIgnoringWhitespace(expectedDefinition);
    }
}