package no.ssb.lds.graphql.schemas;

import graphql.schema.DataFetcher;
import graphql.schema.GraphQLArgument;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLList;
import graphql.schema.GraphQLNonNull;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLOutputType;
import graphql.schema.GraphQLSchema;
import graphql.schema.GraphQLType;
import graphql.schema.GraphQLTypeReference;
import graphql.schema.GraphQLUnionType;
import no.ssb.lds.api.persistence.json.JsonPersistence;
import no.ssb.lds.api.specification.Specification;
import no.ssb.lds.api.specification.SpecificationElement;
import no.ssb.lds.api.specification.SpecificationElementType;
import no.ssb.lds.graphql.fetcher.PersistenceFetcher;
import no.ssb.lds.graphql.fetcher.PersistenceLinkFetcher;
import no.ssb.lds.graphql.fetcher.PersistenceLinksFetcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

import static graphql.Scalars.GraphQLBoolean;
import static graphql.Scalars.GraphQLFloat;
import static graphql.Scalars.GraphQLID;
import static graphql.Scalars.GraphQLLong;
import static graphql.Scalars.GraphQLString;
import static java.lang.String.format;

/**
 * Converts a LDS specification to GraphQL schema.
 */
public class GraphqlSchemaBuilder {

    private static final Logger log = LoggerFactory.getLogger(GraphqlSchemaBuilder.class);
    private final Specification specification;

    // Keep track of the union types we registered.
    private final Set<String> unionTypes = new HashSet<>();

    private final JsonPersistence persistence;
    private final String nameSpace;

    public GraphqlSchemaBuilder(Specification specification, JsonPersistence persistence, String nameSpace) {
        this.specification = Objects.requireNonNull(specification);
        this.persistence = Objects.requireNonNull(persistence);
        this.nameSpace = Objects.requireNonNull(nameSpace);
        if (this.nameSpace.isEmpty()) {
            throw new IllegalArgumentException("namespace was empty");
        }
    }

    /**
     * Returns true if the element is nullable (has null as allowed type).
     */
    private static Boolean isNullable(SpecificationElement element) {
        return elementJsonTypes(element).contains(JsonType.NULL);
    }

    /**
     * Returns type safe element types.
     */
    private static Set<JsonType> elementJsonTypes(SpecificationElement element) {
        LinkedHashSet<JsonType> types = new LinkedHashSet<>();
        for (String jsonType : element.getJsonTypes()) {
            types.add(JsonType.valueOf(jsonType.toUpperCase()));
        }
        return types;
    }

    /**
     * Returns type safe element type.
     * <p>
     * Null type, since it is used to mark fields as nullable is ignored.
     */
    private static JsonType elementJsonType(SpecificationElement element) {
        Set<JsonType> types = elementJsonTypes(element);
        types.remove(JsonType.NULL);
        if (types.size() != 1) {
            throw new IllegalArgumentException(format(
                    "more than one json type in %s: %s", element.getName(), types
            ));
        }
        return types.iterator().next();
    }

    private static String getOneRefType(SpecificationElement property) {
        Set<String> types = property.getRefTypes();
        if (types.size() != 1) {
            throw new IllegalArgumentException(format("More than one ref type for property %s", property.getName()));
        }
        return types.iterator().next();
    }

    /**
     * Helper method that create a {@link GraphQLFieldDefinition.Builder} with name and description.
     */
    private static GraphQLFieldDefinition.Builder createFieldDefinition(SpecificationElement property) {
        GraphQLFieldDefinition.Builder field = GraphQLFieldDefinition.newFieldDefinition();
        field.name(property.getName());
        field.description(property.getDescription());
        return field;
    }

    /**
     * Build a {@link GraphQLSchema}
     */
    public GraphQLSchema getSchema() {

        Set<GraphQLType> additionalTypes = new LinkedHashSet<>();
        GraphQLObjectType.Builder queryBuilder = GraphQLObjectType.newObject().name("Query");

        SpecificationElement root = specification.getRootElement();
        for (SpecificationElement element : root.getProperties().values()) {

            try {
                GraphQLObjectType buildType = createObjectType(element)
                        .build();

                GraphQLFieldDefinition.Builder rootQueryField = createRootQueryField(element);
                DataFetcher rootQueryFetcher = createRootQueryFetcher(element);
                queryBuilder.field(rootQueryField.dataFetcher(rootQueryFetcher).build());
                log.debug("Converted {} to GraphQL type {}", element.getName(), buildType);

                additionalTypes.add(buildType);
            } catch (Exception ex) {
                log.error("could not convert {}", element.getName(), ex);
                throw ex;
            }
        }

        return GraphQLSchema.newSchema().query(queryBuilder.build()).additionalTypes(additionalTypes).build();
    }

    private DataFetcher createRootQueryFetcher(SpecificationElement element) {
        return new PersistenceFetcher(persistence, this.nameSpace, element.getName());
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
                                .type(new GraphQLNonNull(GraphQLID))
                                .build()
                )
                .type(GraphQLTypeReference.typeRef(element.getName()));
    }

    /**
     * Create GraphQL object type from {@link SpecificationElement}
     */
    public GraphQLObjectType.Builder createObjectType(SpecificationElement specificationElement) {
        try {
            GraphQLObjectType.Builder object = GraphQLObjectType.newObject();
            String objectName = specificationElement.getName();
            object.name(objectName);
            object.description(specificationElement.getDescription());

            // For each property
            for (SpecificationElement property : specificationElement.getProperties().values()) {
                GraphQLFieldDefinition.Builder field = buildField(property);

                object.field(field);
            }

            return object;
        } catch (IllegalArgumentException iae) {
            throw new IllegalArgumentException(
                    format(
                            "could not create GraphQLObjectType for %s: %s",
                            specificationElement.getName(), iae.getMessage()),
                    iae
            );
        }
    }

    /**
     * Create a {@link GraphQLFieldDefinition.Builder} from {@link SpecificationElement}
     */
    private GraphQLFieldDefinition.Builder buildField(SpecificationElement property) {
        SpecificationElementType elementType = property.getSpecificationElementType();
        switch (elementType) {
            case EMBEDDED:
                return buildEmbeddedField(property);
            case REF:
                return buildReferenceField(property);
            case ROOT:
            case MANAGED:
            default:
                throw new IllegalArgumentException(format(
                        "property %s was of type %s",
                        property.getName(), elementType
                ));
        }
    }

    /**
     * Create a {@link GraphQLFieldDefinition.Builder} that is a reference (linked) {@link SpecificationElement}.
     */
    private GraphQLFieldDefinition.Builder buildReferenceField(SpecificationElement property) {

        GraphQLFieldDefinition.Builder field = createFieldDefinition(property);

        GraphQLOutputType graphQLOutputType = buildReferenceTargetType(property);
        String propertyName = property.getName();
        JsonType propertyType = elementJsonType(property);
        switch (propertyType) {
            case ARRAY:
                field.type(GraphQLList.list(graphQLOutputType));
                field.dataFetcher(new PersistenceLinksFetcher(
                        persistence,
                        this.nameSpace,
                        propertyName, graphQLOutputType.getName()
                ));
                return field;
            case STRING:
                field.type(graphQLOutputType);
                field.dataFetcher(new PersistenceLinkFetcher(
                        persistence,
                        this.nameSpace,
                        propertyName, graphQLOutputType.getName()
                ));
                return field;
            default:
                throw new IllegalArgumentException(format(
                        "reference %s was of type %s",
                        propertyName, propertyType
                ));
        }
    }

    /**
     * Create the referenced {@link GraphQLOutputType} of a reference (linked) {@link SpecificationElement}.
     */
    private GraphQLOutputType buildReferenceTargetType(SpecificationElement property) {
        String propertyName = property.getName();
        // If more than one type in ref, try to create a Union type.
        if (property.getRefTypes().size() > 1) {
            if (unionTypes.contains(propertyName)) {
                return GraphQLTypeReference.typeRef(propertyName);
            } else {
                GraphQLUnionType.Builder unionType = GraphQLUnionType.newUnionType()
                        .name(propertyName);
                for (String refType : property.getRefTypes()) {
                    unionType.possibleType(GraphQLTypeReference.typeRef(refType));
                }
                // TODO: Handle abstract type.
                unionType.typeResolver(env -> {
                    throw new UnsupportedOperationException("Abstract type not supported yet");
                });
                unionTypes.add(propertyName);
                return unionType.build();
            }
        } else {
            String refType = getOneRefType(property);
            return GraphQLTypeReference.typeRef(refType);
        }
    }

    /**
     * Create an embedded {@link GraphQLFieldDefinition.Builder}.
     * <p>
     * Nullability is checked using {@link #isNullable(SpecificationElement)}.
     */
    private GraphQLFieldDefinition.Builder buildEmbeddedField(SpecificationElement property) {

        GraphQLFieldDefinition.Builder field = createFieldDefinition(property);
        GraphQLOutputType propertyType = buildEmbeddedTargetType(property);

        if (isNullable(property)) {
            field.type(propertyType);
        } else {
            field.type(new GraphQLNonNull(propertyType));
        }

        return field;
    }

    /**
     * Returns a {@link GraphQLOutputType} of an embedded {@link SpecificationElement}.
     * <p>
     * If the type is object, the method recurse.
     */
    private GraphQLOutputType buildEmbeddedTargetType(SpecificationElement property) {
        JsonType jsonType = elementJsonType(property);
        switch (jsonType) {
            case OBJECT:
                // Recurse if embedded.
                return createObjectType(property).build();
            case ARRAY:
                // TODO: Ideally we should recurse.
                SpecificationElement arrayElement = property.getItems();
                JsonType arrayType = elementJsonType(arrayElement);
                if (arrayType == JsonType.OBJECT && !"".equals(arrayElement.getName())) {
                    String refType = arrayElement.getName();
                    return GraphQLList.list(GraphQLTypeReference.typeRef(refType));
                } else {
                    // TODO: Array can be of scalar type.
                    return GraphQLList.list(GraphQLString);
                }
            case STRING:
                return GraphQLString;
            case NUMBER:
                return GraphQLFloat;
            case BOOLEAN:
                return GraphQLBoolean;
            case INTEGER:
                return GraphQLLong;
        }
        throw new AssertionError();
    }

    public enum JsonType {
        OBJECT,
        ARRAY,
        BOOLEAN,
        STRING,
        NUMBER,
        INTEGER,
        NULL
    }

}
