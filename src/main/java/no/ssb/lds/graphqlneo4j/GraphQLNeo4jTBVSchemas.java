package no.ssb.lds.graphqlneo4j;

import graphql.language.Directive;
import graphql.language.ObjectTypeDefinition;
import graphql.language.TypeDefinition;
import graphql.schema.GraphQLCodeRegistry;
import graphql.schema.GraphQLDirective;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLModifiedType;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLSchema;
import graphql.schema.GraphQLType;
import graphql.schema.GraphQLTypeCollectingVisitor;
import graphql.schema.GraphQLTypeResolvingVisitor;
import graphql.schema.TypeTraverser;
import graphql.schema.idl.TypeDefinitionRegistry;
import no.ssb.lds.graphql.directives.DomainDirective;
import no.ssb.lds.graphql.schemas.visitors.TypeReferencerVisitor;
import org.neo4j.graphql.Cypher;
import org.neo4j.graphql.SchemaBuilder;
import org.neo4j.graphql.SchemaConfig;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.util.Optional.ofNullable;

public class GraphQLNeo4jTBVSchemas {

    /**
     * Returns a GraphQL-schema that will produce cypher mutations and queries compatible with time-based-versioning.
     *
     * @param typeDefinitionRegistry
     * @return the time-based-versioning compatible GraphQL-schema
     */
    public static GraphQLSchema schemaOf(TypeDefinitionRegistry typeDefinitionRegistry) {
        final Set<String> queryTypes = new CopyOnWriteArraySet<>();

        TypeDefinitionRegistry withoutDomainDirectives = new TypeDefinitionRegistry().merge(typeDefinitionRegistry);
        for (Map.Entry<String, TypeDefinition> typeByName : typeDefinitionRegistry.types().entrySet()) {
            TypeDefinition typeDefinition = typeByName.getValue();
            if (typeDefinition instanceof ObjectTypeDefinition) {
                List<Directive> directives = typeDefinition.getDirectives();
                if (directives.removeIf(d -> d.getName().equals("domain"))) {
                    ObjectTypeDefinition transformedTypeDefinition = ((ObjectTypeDefinition) typeDefinition).transform(builder -> builder.directives(directives));
                    withoutDomainDirectives.remove(typeDefinition);
                    withoutDomainDirectives.add(transformedTypeDefinition);
                }
            }
        }

        GraphQLSchema graphQLSchema = SchemaBuilder.buildSchema(withoutDomainDirectives,
                new SchemaConfig(new SchemaConfig.CRUDConfig(true, Collections.emptyList()), new SchemaConfig.CRUDConfig(false, Collections.emptyList())),
                (dataFetchingEnvironment, dataFetcher) -> {
                    String name = dataFetchingEnvironment.getField().getName();
                    Cypher cypher = dataFetcher.get(dataFetchingEnvironment);
                    if (queryTypes.contains(name)) {
                        String type = unwrapGraphQLTypeAndGetName(dataFetchingEnvironment.getFieldDefinition().getType());
                        String regex = String.format("MATCH\\s*\\(%s:%s\\)(\\s*WHERE)?(\\s*)(?:(?!RETURN).)*\\s*RETURN", name, type);
                        Matcher m = Pattern.compile(regex).matcher(cypher.component1());
                        if (!m.find()) {
                            throw new IllegalArgumentException("Generated Cypher does not match regex");
                        }
                        boolean where = ofNullable(m.group(1)).map(g -> true).orElse(false);
                        String query = replaceGroup(regex, cypher.component1(), 2, where
                                ? " (_v.from <= $_version AND coalesce($_version < _v.to, true)) AND "
                                : " WHERE (_v.from <= $_version AND coalesce($_version < _v.to, true)) ");
                        query = replaceGroup(String.format("MATCH\\s*(\\(%s:%s\\))\\s*WHERE", name, type), query, 1, String.format("(_r:%s:RESOURCE)<-[_v:VERSION_OF]-(%s:%s:INSTANCE)", type + "_R", name, type));
                        return new Cypher(query, cypher.component2(), cypher.component3());
                    }
                    return cypher;
                });

        final TypeTraverser TRAVERSER = new TypeTraverser();

        Map<String, GraphQLType> graphQLTypes = new TreeMap<>(graphQLSchema.getTypeMap());
        graphQLTypes.keySet().removeIf(key -> key.startsWith("__"));

        GraphQLTypeCollectingVisitor graphQLTypeCollectingVisitor = new GraphQLTypeCollectingVisitor();
        TRAVERSER.depthFirst(graphQLTypeCollectingVisitor, graphQLTypes.values());
        Map<String, GraphQLType> typeMap = graphQLTypeCollectingVisitor.getResult();

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
        queryTypes.addAll(transformedGraphQLSchema.getQueryType().getChildren().stream().map(GraphQLType::getName).collect(Collectors.toSet()));

        return transformedGraphQLSchema;
    }

    private static void addDomainDirective(TypeDefinitionRegistry typeDefinitionRegistry, Map<String, GraphQLType> typeMap) {
        // add domain directive
        for (Map.Entry<String, GraphQLType> entry : typeMap.entrySet()) {
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
        Set<? extends String> originalQueries = queryType.getChildren().stream().map(GraphQLType::getName).collect(Collectors.toSet());
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
        return type.getName();
    }

    public static String replaceGroup(String regex, String source, int groupToReplace, String replacement) {
        return replaceGroup(regex, source, groupToReplace, 1, replacement);
    }

    public static String replaceGroup(String regex, String source, int groupToReplace, int groupOccurrence, String replacement) {
        Matcher m = Pattern.compile(regex).matcher(source);
        for (int i = 0; i < groupOccurrence; i++)
            if (!m.find()) return source; // pattern not met, may also throw an exception here
        return new StringBuilder(source).replace(m.start(groupToReplace), m.end(groupToReplace), replacement).toString();
    }
}
