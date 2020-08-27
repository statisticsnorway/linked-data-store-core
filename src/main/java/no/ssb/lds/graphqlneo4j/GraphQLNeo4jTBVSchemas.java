package no.ssb.lds.graphqlneo4j;

import graphql.GraphQLError;
import graphql.language.Argument;
import graphql.language.Directive;
import graphql.language.EnumTypeDefinition;
import graphql.language.FieldDefinition;
import graphql.language.InputObjectTypeDefinition;
import graphql.language.InputValueDefinition;
import graphql.language.InterfaceTypeDefinition;
import graphql.language.ListType;
import graphql.language.NonNullType;
import graphql.language.ObjectTypeDefinition;
import graphql.language.ScalarTypeDefinition;
import graphql.language.StringValue;
import graphql.language.Type;
import graphql.language.TypeDefinition;
import graphql.language.TypeName;
import graphql.language.UnionTypeDefinition;
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
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class GraphQLNeo4jTBVSchemas {

    /**
     * Returns a transformed copy of the source-registry. The transformations occur on types that have link directive
     * set, and will replace this the link directive with a cypher directive capable of resolving time-base-versioning
     * at query time.
     *
     * @param sourceRegistry the type-registry to be transformed. Note that the sourceRegistry instance itself is left
     *                       unchanged, the returned registry is a transformed copy of the source.
     * @return a new type-registry with relevant types transformed to support time-based-versioning
     */
    public static TypeDefinitionRegistry transformRegistry(TypeDefinitionRegistry sourceRegistry) {
        final TypeDefinitionRegistry typeDefinitionRegistry = new TypeDefinitionRegistry().merge(sourceRegistry);

        typeDefinitionRegistry.scalars().entrySet().forEach(entry -> {
            ScalarTypeDefinition type = entry.getValue();
            if (Set.of("Date", "Time", "DateTime").contains(entry.getKey())) {
                typeDefinitionRegistry.remove(type);
            }
        });

        replaceDateTimeWithNeo4Types(typeDefinitionRegistry);

        addRelationDirectives(typeDefinitionRegistry);

        replaceUnionsWithInterfaces(typeDefinitionRegistry);

        return typeDefinitionRegistry;
    }

    private static void replaceDateTimeWithNeo4Types(TypeDefinitionRegistry typeDefinitionRegistry) {
        typeDefinitionRegistry.types().entrySet().forEach(entry -> {
            TypeDefinition type = entry.getValue();
            if (!(type instanceof ObjectTypeDefinition
                    || type instanceof InterfaceTypeDefinition)) {
                return;
            }
            Map<String, FieldDefinition> transformedFields = new LinkedHashMap<>();

            type.getChildren().forEach(child -> {
                if (!(child instanceof FieldDefinition)) {
                    return;
                }
                FieldDefinition field = (FieldDefinition) child;
                Type fieldType = field.getType();
                String typeName = unwrapTypeAndGetName(fieldType);
                if (Set.of("Date", "Time", "DateTime").contains(typeName)) {
                    String transformedTypeName = "_Neo4j" + typeName;
                    transformedFields.put(field.getName(), field.transform(builder -> {
                        Type transformedType = transformTypeChain(fieldType, transformedTypeName);
                        builder.type(transformedType);
                    }));
                }
            });

            replaceTransformedFieldsInType(typeDefinitionRegistry, type, transformedFields);
        });
    }

    private static Type transformTypeChain(Type type, String requestedNewInnerTypeName) {
        if (type instanceof ListType) {
            Type wrappedType = transformTypeChain(((ListType) type).getType(), requestedNewInnerTypeName);
            return ListType.newListType(wrappedType).build();
        }
        if (type instanceof NonNullType) {
            Type wrappedType = transformTypeChain(((NonNullType) type).getType(), requestedNewInnerTypeName);
            return NonNullType.newNonNullType(wrappedType).build();
        }
        if (type instanceof TypeName) {
            return TypeName.newTypeName(requestedNewInnerTypeName).build();
        }
        throw new IllegalArgumentException("transformation of type not supported: " + type.getClass().getName());
    }

    private static void addRelationDirectives(TypeDefinitionRegistry typeDefinitionRegistry) {
        typeDefinitionRegistry.types().entrySet().forEach(entry -> {
            String nameOfType = entry.getKey();
            TypeDefinition type = entry.getValue();
            if (!(type instanceof ObjectTypeDefinition
                    || type instanceof InterfaceTypeDefinition)) {
                return;
            }
            Map<String, FieldDefinition> transformedFields = new LinkedHashMap<>();
            type.getChildren().forEach(child -> {
                if (!(child instanceof FieldDefinition)) {
                    return;
                }
                FieldDefinition field = (FieldDefinition) child;
                boolean isLink = field.getDirective("link") != null;
                if (isLink) {

                    String targetType = null;
                    if (field.getType() instanceof ListType) {
                        Type nestedType = ((ListType) field.getType()).getType();
                        if (nestedType instanceof TypeName) {
                            targetType = ((TypeName) nestedType).getName();
                        } else {
                            throw new IllegalArgumentException("Error in " + nameOfType + "." + field.getName() + " : nested list-target type is not a TypeName");
                        }
                    } else if (field.getType() instanceof TypeName) {
                        targetType = ((TypeName) field.getType()).getName();
                    }

                    String relationName = field.getName();

                    String tbvResolutionCypher = String.format("MATCH (this)-[:%s]->(:%s_R)<-[v:VERSION_OF]-(n:%s) WHERE v.from <= ver AND coalesce(ver < v.to, true) RETURN n", relationName, targetType, targetType);

                    FieldDefinition transformedField = field.transform(builder -> builder
                            .directives(List.of(
                                    field.getDirective("link"),
                                    Directive.newDirective()
                                            .name("cypher")
                                            .arguments(List.of(Argument.newArgument()
                                                    .name("statement")
                                                    .value(StringValue.newStringValue()
                                                            .value(tbvResolutionCypher)
                                                            .build())
                                                    .build()))
                                            .build()
                            ))
                            .inputValueDefinitions(List.of(InputValueDefinition.newInputValueDefinition()
                                    .name("ver")
                                    .type(new TypeName("_Neo4jDateTimeInput"))
                                    .build()))
                    );
                    transformedFields.put(field.getName(), transformedField);
                } else {
                    TypeDefinition typeDefinition = typeDefinitionRegistry.getType(field.getType()).orElse(null);
                    if (typeDefinition == null) {
                        return;
                    }
                    if (typeDefinition instanceof ScalarTypeDefinition) {
                    } else if (typeDefinition instanceof EnumTypeDefinition) {
                    } else if (typeDefinition instanceof ObjectTypeDefinition
                            || typeDefinition instanceof InterfaceTypeDefinition) {
                        FieldDefinition transformedField = field.transform(builder -> builder.directive(Directive.newDirective()
                                .name("relation")
                                .arguments(List.of(Argument.newArgument()
                                        .name("name")
                                        .value(StringValue.newStringValue()
                                                .value(field.getName())
                                                .build())
                                        .build()))
                                .build()
                        ));
                        transformedFields.put(field.getName(), transformedField);
                    } else if (typeDefinition instanceof UnionTypeDefinition) {
                    } else if (typeDefinition instanceof InputObjectTypeDefinition) {
                    } else {
                        throw new UnsupportedOperationException("Unknown concrete TypeDefinition class: " + typeDefinition.getClass().getName());
                    }
                }
            });

            replaceTransformedFieldsInType(typeDefinitionRegistry, type, transformedFields);
        });
    }

    private static void replaceUnionsWithInterfaces(TypeDefinitionRegistry typeDefinitionRegistry) {
        for (Map.Entry<String, UnionTypeDefinition> entry : typeDefinitionRegistry.getTypesMap(UnionTypeDefinition.class).entrySet()) {
            String unionName = entry.getKey();
            UnionTypeDefinition typeDefinition = entry.getValue();
            InterfaceTypeDefinition interfaceTypeDefinition = InterfaceTypeDefinition
                    .newInterfaceTypeDefinition()
                    .name(unionName)
                    .build();
            typeDefinitionRegistry.remove(typeDefinition);
            Optional<GraphQLError> optionalError = typeDefinitionRegistry.add(interfaceTypeDefinition);
            if (optionalError.isPresent()) {
                throw new RuntimeException(optionalError.get().getMessage());
            }
            for (Type typeReference : typeDefinition.getMemberTypes()) {
                String typeName = unwrapTypeAndGetName(typeReference);
                TypeDefinition resolvedType = typeDefinitionRegistry.getType(typeName).orElseThrow();
                if (!(resolvedType instanceof ObjectTypeDefinition)) {
                    throw new IllegalArgumentException("Union '" + unionName + "' points to non-object type '" + resolvedType.getName() + "'");
                }
                ObjectTypeDefinition transformedOjectType = ((ObjectTypeDefinition) resolvedType).transform(builder -> builder
                        .implementz(TypeName.newTypeName(unionName).build())
                );
                typeDefinitionRegistry.remove(resolvedType);
                typeDefinitionRegistry.add(transformedOjectType);
            }
        }
    }

    private static void replaceTransformedFieldsInType(TypeDefinitionRegistry typeDefinitionRegistry, TypeDefinition type, Map<String, FieldDefinition> transformedFields) {
        if (transformedFields.size() > 0) {
            if (type instanceof ObjectTypeDefinition) {
                ObjectTypeDefinition transformedObjectType = ((ObjectTypeDefinition) type).transform(builder ->
                        builder.fieldDefinitions(((ObjectTypeDefinition) type).getFieldDefinitions().stream()
                                .map(field -> Optional.ofNullable(transformedFields.get(field.getName())).orElse(field))
                                .collect(Collectors.toList())
                        )
                );
                typeDefinitionRegistry.remove(type);
                typeDefinitionRegistry.add(transformedObjectType);
            } else if (type instanceof InterfaceTypeDefinition) {
                InterfaceTypeDefinition transformedObjectType = ((InterfaceTypeDefinition) type).transform(builder ->
                        builder.definitions(((InterfaceTypeDefinition) type).getFieldDefinitions().stream()
                                .map(field -> Optional.ofNullable(transformedFields.get(field.getName())).orElse(field))
                                .collect(Collectors.toList())
                        )
                );
                typeDefinitionRegistry.remove(type);
                typeDefinitionRegistry.add(transformedObjectType);
            }
        }
    }

    private static String unwrapTypeAndGetName(Type type) {
        if (type instanceof NonNullType) {
            return unwrapTypeAndGetName(((NonNullType) type).getType());
        }
        if (type instanceof ListType) {
            return unwrapTypeAndGetName(((ListType) type).getType());
        }
        return ((TypeName) type).getName(); // instance must be an instanceof TypeName
    }

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
                        String query = replaceGroup(String.format("MATCH \\(%s:%s\\) WHERE( )", name, type), cypher.component1(), 1, " (_v.from <= $_version AND coalesce($_version < _v.to, true)) AND ");
                        query = replaceGroup(String.format("MATCH (\\(%s:%s\\)) WHERE", name, type), query, 1, String.format("(_r:%s:RESOURCE)<-[_v:VERSION_OF]-(%s:%s:INSTANCE)", type + "_R", name, type));
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
