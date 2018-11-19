package no.ssb.lds.graphql;

import graphql.schema.GraphQLArgument;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLList;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLOutputType;
import graphql.schema.GraphQLSchema;
import graphql.schema.GraphQLType;
import graphql.schema.GraphQLTypeReference;
import graphql.schema.GraphQLUnionType;
import graphql.schema.StaticDataFetcher;
import no.ssb.lds.core.specification.Specification;
import no.ssb.lds.core.specification.SpecificationElement;
import no.ssb.lds.core.specification.SpecificationElementType;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static graphql.Scalars.GraphQLBoolean;
import static graphql.Scalars.GraphQLFloat;
import static graphql.Scalars.GraphQLID;
import static graphql.Scalars.GraphQLLong;
import static graphql.Scalars.GraphQLString;
import static java.lang.String.format;
import static no.ssb.lds.core.specification.SpecificationElementType.EMBEDDED;
import static no.ssb.lds.core.specification.SpecificationElementType.REF;

/**
 * Converts a LDS specification to GraphQL schema.
 */
public class GraphqlSchemaBuilder {

    private static final Logger log = LoggerFactory.getLogger(GraphqlSchemaBuilder.class);
    private final Specification specification;
    private final Set<String> unionTypes = new HashSet<>();

    public GraphqlSchemaBuilder(Specification specification) {
        this.specification = Objects.requireNonNull(specification);
    }

    private static Boolean isRequired(SpecificationElement property) {
        return property.getJsonTypes().contains("null");
    }

    /**
     * TODO: Fix this.
     */
    private static String getOneJsonType(SpecificationElement property) {
        if (!isRequired(property)) {
            Set<String> types = property.getJsonTypes();
            if (types.size() != 1) {
                throw new IllegalArgumentException("more than one json type");
            }
            return types.iterator().next();
        } else {
            Set<String> types = property.getJsonTypes();
            if (types.size() != 2) {
                throw new IllegalArgumentException("unsupported type");
            }
            Iterator<String> iterator = types.iterator();
            String next = iterator.next();
            if ("null".equals(next)) {
                return iterator.next();
            } else {
                return next;
            }
        }
    }

    private static String getOneRefType(SpecificationElement property) {
        Set<String> types = property.getRefTypes();
        if (types.size() != 1) {
            throw new IllegalArgumentException(format("More than one ref type for property %s", property.getName()));
        }
        return types.iterator().next();
    }

    // Build a graphql schema out of the specification.
    public GraphQLSchema getSchema() {

        Set<GraphQLType> additionalTypes = new LinkedHashSet<>();
        GraphQLObjectType.Builder queryBuilder = GraphQLObjectType.newObject().name("Query");

        SpecificationElement root = specification.getRootElement();
        for (SpecificationElement element : root.getProperties().values()) {

            try {
                GraphQLObjectType buildType = createObjectType(element)
                        .build();

                GraphQLFieldDefinition.Builder rootQueryField = createRootQueryField(element);
                StaticDataFetcher rootQueryFetcher = createRootQueryFetcher(element);
                queryBuilder.field(rootQueryField.dataFetcher(rootQueryFetcher).build());
                log.debug("Converted {} to graphql type {}", element.getName(), buildType);

                additionalTypes.add(buildType);
            } catch (Exception ex) {
                log.error("could not convert {}", element.getName(), ex);
                throw ex;
            }
        }

        return GraphQLSchema.newSchema().query(queryBuilder.build()).additionalTypes(additionalTypes).build();
    }

    private StaticDataFetcher createRootQueryFetcher(SpecificationElement element) {
        return new StaticDataFetcher(Arrays.asList(
                new JSONObject(Map.of("name", "Hadrien", "type", element.getName())).toMap(),
                new JSONObject(Map.of("name", "Kim")).toMap()
        ));
    }

    /**
     * Creates a root query field with ID argument.
     */
    private GraphQLFieldDefinition.Builder createRootQueryField(SpecificationElement element) {
        return GraphQLFieldDefinition.newFieldDefinition()
                .name(element.getName())
                .argument(
                        GraphQLArgument.newArgument()
                                .name("id")
                                .type(GraphQLID)
                                .build()
                )
                .type(GraphQLList.list(GraphQLTypeReference.typeRef(element.getName())));
    }

    public GraphQLObjectType.Builder createObjectType(SpecificationElement specificationElement) {
        GraphQLObjectType.Builder object = GraphQLObjectType.newObject();
        object.name(specificationElement.getName());
        object.description(specificationElement.getDescription());

        // For each property
        for (SpecificationElement property : specificationElement.getProperties().values()) {

            GraphQLFieldDefinition.Builder field = GraphQLFieldDefinition.newFieldDefinition();
            String typeName = property.getName();
            field.name(typeName);

            SpecificationElementType elementType = property.getSpecificationElementType();
            field.description(property.getDescription());
            if (EMBEDDED.equals(elementType)) {
                String jsonType = getOneJsonType(property);
                if ("object".equals(jsonType)) {
                    // Recurse if embedded.
                    field.type(createObjectType(property));
                    field.dataFetcher(new StaticDataFetcher(new JSONObject()));
                }
                if ("boolean".equals(jsonType)) {
                    field.type(GraphQLList.list(GraphQLBoolean));
                } else if ("array".equals(jsonType)) {
                    SpecificationElement arrayType = property.getItems();
                    String type = getOneJsonType(arrayType);
                    if ("object".equals(type) && !"".equals(arrayType.getName())) {
                        String refType = arrayType.getName();
                        field.type(GraphQLList.list(GraphQLTypeReference.typeRef(refType)));
                    } else {
                        field.type(GraphQLList.list(GraphQLString));
                    }
                } else if ("string".equals(jsonType)) {
                    field.type(GraphQLString);
                } else if ("number".equals(jsonType)) {
                    field.type(GraphQLFloat);
                } else if ("integer".equals(jsonType)) {
                    field.type(GraphQLLong);
                }
            } else if (REF.equals(elementType)) {
                GraphQLOutputType graphQLOutputType;
                // If more than one type in ref, try to create a Union type.
                if (property.getRefTypes().size() > 1) {
                    if (unionTypes.contains(typeName)) {
                        graphQLOutputType = GraphQLTypeReference.typeRef(typeName);
                    } else {
                        GraphQLUnionType.Builder unionType = GraphQLUnionType.newUnionType()
                                .name(typeName);
                        for (String refType : property.getRefTypes()) {
                            unionType.possibleType(GraphQLTypeReference.typeRef(refType));
                        }
                        // TODO: Handle abstract type.
                        unionType.typeResolver(env -> {
                            throw new UnsupportedOperationException("Abstract type not supported yet");
                        });
                        graphQLOutputType = unionType.build();
                        unionTypes.add(typeName);
                    }

                } else {
                    String refType = getOneRefType(property);
                    graphQLOutputType = GraphQLTypeReference.typeRef(refType);
                }
                String jsonType = getOneJsonType(property);
                if ("array".equals(jsonType)) {
                    field.type(GraphQLList.list(graphQLOutputType));
                } else if ("string".equals(jsonType)) {
                    field.type(graphQLOutputType);
                }
            } else {
                throw new AssertionError();
            }

            object.field(field);
        }

        return object;
    }

}
