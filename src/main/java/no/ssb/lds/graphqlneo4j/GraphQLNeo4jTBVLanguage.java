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
import graphql.schema.idl.TypeDefinitionRegistry;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class GraphQLNeo4jTBVLanguage {

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

        typeDefinitionRegistry.scalars().forEach((key, type) -> {
            if (Set.of("Date", "Time", "DateTime").contains(key)) {
                typeDefinitionRegistry.remove(type);
            }
        });

        replaceDateTimeWithNeo4Types(typeDefinitionRegistry);

        replaceUnionsWithInterfaces(typeDefinitionRegistry);

        addLinkCypherAndEmbeddedRelationDirectives(typeDefinitionRegistry);

        return typeDefinitionRegistry;
    }

    private static void replaceDateTimeWithNeo4Types(TypeDefinitionRegistry typeDefinitionRegistry) {
        typeDefinitionRegistry.types().forEach((key, type) -> {
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

    private static void addLinkCypherAndEmbeddedRelationDirectives(TypeDefinitionRegistry typeDefinitionRegistry) {
        typeDefinitionRegistry.types().forEach((nameOfType, type) -> {
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
                    String relationName = field.getName();
                    String tbvResolutionCypher = String.format("MATCH (this)-[:%s]->(:RESOURCE)<-[v:VERSION_OF]-(n) WHERE v.from <= ver AND coalesce(ver < v.to, true) RETURN n", relationName);

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

    public static String replaceGroup(String regex, String source, int groupToReplace, String replacement) {
        return replaceGroup(regex, source, groupToReplace, 1, replacement);
    }

    public static String replaceGroup(String regex, String source, int groupToReplace, int groupOccurrence, String replacement) {
        Matcher m = Pattern.compile(regex).matcher(source);
        for (int i = 0; i < groupOccurrence; i++)
            if (!m.find()) return source; // pattern not met, may also throw an exception here
        return new StringBuilder(source).replace(m.start(groupToReplace), m.end(groupToReplace), replacement).toString();
    }

    public static List<String> resolveAbstractTypeToConcreteTypes(TypeDefinitionRegistry typeDefinitionRegistry, String abstractTypeName) {
        TypeDefinition abstractType = typeDefinitionRegistry.getType(abstractTypeName).orElseThrow();
        if (abstractType instanceof ObjectTypeDefinition) {
            return List.of(abstractType.getName());
        } else if (abstractType instanceof UnionTypeDefinition) {
            return ((UnionTypeDefinition) abstractType).getMemberTypes().stream()
                    .map(mt -> (TypeName) mt)
                    .map(TypeName::getName)
                    .flatMap(name -> resolveAbstractTypeToConcreteTypes(typeDefinitionRegistry, name).stream())
                    .collect(Collectors.toList());
        } else if (abstractType instanceof InterfaceTypeDefinition) {
            return typeDefinitionRegistry.getTypes(ObjectTypeDefinition.class).stream()
                    .filter(otd -> otd.getImplements().stream()
                            .map(ii -> (TypeName) ii)
                            .map(TypeName::getName)
                            .anyMatch(name -> name.equals(abstractTypeName)))
                    .map(ObjectTypeDefinition::getName)
                    .collect(Collectors.toList());
        }
        throw new IllegalArgumentException("Unsupported abstract type: " + abstractType.getClass().getName());
    }
}
