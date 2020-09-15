package no.ssb.lds.graphqlneo4j;

import graphql.language.Directive;
import graphql.language.ObjectTypeDefinition;
import graphql.language.TypeDefinition;
import graphql.schema.GraphQLCodeRegistry;
import graphql.schema.GraphQLDirective;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLModifiedType;
import graphql.schema.GraphQLNamedSchemaElement;
import graphql.schema.GraphQLNamedType;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLSchema;
import graphql.schema.GraphQLType;
import graphql.schema.GraphQLTypeCollectingVisitor;
import graphql.schema.GraphQLTypeResolvingVisitor;
import graphql.schema.SchemaTraverser;
import graphql.schema.idl.TypeDefinitionRegistry;
import no.ssb.lds.graphql.directives.DomainDirective;
import no.ssb.lds.graphql.schemas.visitors.TypeReferencerVisitor;
import org.neo4j.graphql.Cypher;
import org.neo4j.graphql.SchemaBuilder;
import org.neo4j.graphql.SchemaConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.stream.Collectors;

public class GraphQLNeo4jTBVSchemas {

    private static final Logger LOG = LoggerFactory.getLogger(GraphQLNeo4jTBVSchemas.class);

    public static Set<String> domains(TypeDefinitionRegistry typeDefinitionRegistry) {
        Set<String> domains = new LinkedHashSet<>();
        for (Map.Entry<String, TypeDefinition> typeByName : typeDefinitionRegistry.types().entrySet()) {
            TypeDefinition typeDefinition = typeByName.getValue();
            if (typeDefinition instanceof ObjectTypeDefinition) {
                List<Directive> directives = typeDefinition.getDirectives();
                if (directives.removeIf(d -> d.getName().equals("domain"))) {
                    domains.add(typeDefinition.getName());
                }
            }
        }
        return domains;
    }

    /**
     * Returns a GraphQL-schema that will produce cypher mutations and queries compatible with time-based-versioning.
     *
     * @param typeDefinitionRegistry
     * @return the time-based-versioning compatible GraphQL-schema
     */
    public static GraphQLSchema schemaOf(TypeDefinitionRegistry typeDefinitionRegistry) {
        final Set<String> queryTypes = new CopyOnWriteArraySet<>();

        Set<String> domains = new LinkedHashSet<>();
        TypeDefinitionRegistry withoutDomainDirectives = new TypeDefinitionRegistry().merge(typeDefinitionRegistry);
        for (Map.Entry<String, TypeDefinition> typeByName : typeDefinitionRegistry.types().entrySet()) {
            TypeDefinition typeDefinition = typeByName.getValue();
            if (typeDefinition instanceof ObjectTypeDefinition) {
                List<Directive> directives = typeDefinition.getDirectives();
                if (directives.removeIf(d -> d.getName().equals("domain"))) {
                    ObjectTypeDefinition transformedTypeDefinition = ((ObjectTypeDefinition) typeDefinition).transform(builder -> builder.directives(directives));
                    withoutDomainDirectives.remove(typeDefinition);
                    withoutDomainDirectives.add(transformedTypeDefinition);
                    domains.add(typeDefinition.getName());
                }
            }
        }

        GraphQLSchema graphQLSchema = SchemaBuilder.buildSchema(withoutDomainDirectives,
                new SchemaConfig(new SchemaConfig.CRUDConfig(true, Collections.emptyList()), new SchemaConfig.CRUDConfig(false, Collections.emptyList())),
                (dataFetchingEnvironment, dataFetcher) -> {
                    String name = dataFetchingEnvironment.getField().getName();
                    Cypher cypher = dataFetcher.get(dataFetchingEnvironment);
                    if (queryTypes.contains(name)) {
                        String transformedComponent1 = new CypherQueryTransformer(domains).transform(cypher.component1());
                        LOG.trace("CYPHER BEFORE: {}", cypher.component1());
                        LOG.trace("CYPHER AFTER: {}", transformedComponent1);
                        return new Cypher(transformedComponent1, cypher.component2(), cypher.component3());
                    }
                    return cypher;
                });

        final SchemaTraverser TRAVERSER = new SchemaTraverser();

        Map<String, GraphQLNamedType> graphQLTypes = new TreeMap<>(graphQLSchema.getTypeMap());
        graphQLTypes.keySet().removeIf(key -> key.startsWith("__"));

        GraphQLTypeCollectingVisitor graphQLTypeCollectingVisitor = new GraphQLTypeCollectingVisitor();
        TRAVERSER.depthFirst(graphQLTypeCollectingVisitor, graphQLTypes.values());
        Map<String, GraphQLNamedType> typeMap = graphQLTypeCollectingVisitor.getResult();

        TRAVERSER.depthFirst(new TypeReferencerVisitor(typeMap), typeMap.values()); // replace all types with references

        addDomainDirective(typeDefinitionRegistry, typeMap);

        GraphQLObjectType transformedQueryObject = transformQueryObject(typeDefinitionRegistry, (GraphQLObjectType) typeMap.get("Query"));
        typeMap.remove("Query");

        TRAVERSER.depthFirst(new GraphQLTypeResolvingVisitor(typeMap), typeMap.values()); // resolve all references

        LinkedHashSet<GraphQLDirective> directives = new LinkedHashSet<>(graphQLSchema.getDirectives());
        GraphQLCodeRegistry codeRegistry = graphQLSchema.getCodeRegistry();
        GraphQLSchema transformedGraphQLSchema = GraphQLSchema.newSchema()
                .query(transformedQueryObject)
                .additionalTypes(new HashSet<>(typeMap.values()))
                .additionalDirectives(directives)
                .codeRegistry(codeRegistry)
                .build();

        // set of all query types
        queryTypes.addAll(transformedGraphQLSchema.getQueryType().getChildren().stream()
                .filter(se -> se instanceof GraphQLNamedSchemaElement)
                .map(se -> (GraphQLNamedSchemaElement) se)
                .map(GraphQLNamedSchemaElement::getName)
                .collect(Collectors.toSet()));

        return transformedGraphQLSchema;
    }

    private static void addDomainDirective(TypeDefinitionRegistry typeDefinitionRegistry, Map<String, GraphQLNamedType> typeMap) {
        // add domain directive
        for (Map.Entry<String, GraphQLNamedType> entry : typeMap.entrySet()) {
            String typeName = entry.getKey();
            GraphQLType graphQLType = entry.getValue();
            if (graphQLType instanceof GraphQLObjectType) {
                typeDefinitionRegistry.getType(typeName).ifPresent(typeDefinition -> {
                    Directive domainDirective = typeDefinition.getDirective("domain");
                    if (domainDirective != null) {
                        GraphQLObjectType transformedType = GraphQLObjectType.newObject((GraphQLObjectType) graphQLType)
                                .withDirective(DomainDirective.INSTANCE)
                                .build();
                        typeMap.put(transformedType.getName(), transformedType);
                    }
                });
            }
        }
    }

    private static GraphQLObjectType transformQueryObject(TypeDefinitionRegistry typeDefinitionRegistry, GraphQLObjectType queryType) {
        Set<? extends String> originalQueries = queryType.getChildren().stream()
                .filter(se -> se instanceof GraphQLNamedSchemaElement)
                .map(se -> (GraphQLNamedSchemaElement) se)
                .map(GraphQLNamedSchemaElement::getName)
                .collect(Collectors.toSet());
        GraphQLObjectType transformedQueryObject = queryType.transform(graphQLObjectTypeBuilder -> {
            graphQLObjectTypeBuilder.clearFields();
            for (String originalQuery : originalQueries) {
                GraphQLFieldDefinition originalFieldDefinition = queryType.getFieldDefinition(originalQuery);
                String typeName = unwrapGraphQLTypeAndGetName(originalFieldDefinition.getType());
                TypeDefinition typeDefinition = typeDefinitionRegistry.getType(typeName).get();
                if (typeDefinition.getDirective("domain") != null) {
                    graphQLObjectTypeBuilder.field(originalFieldDefinition.transform(fieldDefinitionBuilder -> {
                    }));
                }
            }
        });
        return transformedQueryObject;
    }

    private static String unwrapGraphQLTypeAndGetName(GraphQLType type) {
        if (type instanceof GraphQLModifiedType) {
            return unwrapGraphQLTypeAndGetName(((GraphQLModifiedType) type).getWrappedType());
        }
        if (type instanceof GraphQLNamedType) {
            return ((GraphQLNamedType) type).getName();
        }
        throw new UnsupportedOperationException("Not a named or modified type: " + type.getClass().getName());
    }
}
