package no.ssb.lds.graphql;

import graphql.schema.DataFetcher;
import graphql.schema.GraphQLArgument;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLList;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLOutputType;
import graphql.schema.GraphQLSchema;
import graphql.schema.GraphQLType;
import graphql.schema.GraphQLTypeReference;
import graphql.schema.GraphQLUnionType;
import no.ssb.lds.api.persistence.Persistence;
import no.ssb.lds.core.specification.Specification;
import no.ssb.lds.core.specification.SpecificationElement;
import no.ssb.lds.core.specification.SpecificationElementType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Iterator;
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

    private final Persistence persistence;

    public GraphqlSchemaBuilder(Specification specification, Persistence persistence) {
        this.specification = Objects.requireNonNull(specification);
        this.persistence = Objects.requireNonNull(persistence);
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

    private static GraphQLFieldDefinition.Builder createFieldDefinition(SpecificationElement property) {
        GraphQLFieldDefinition.Builder field = GraphQLFieldDefinition.newFieldDefinition();
        field.name(property.getName());
        field.description(property.getDescription());
        return field;
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
                DataFetcher rootQueryFetcher = createRootQueryFetcher(element);
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

    private DataFetcher createRootQueryFetcher(SpecificationElement element) {
        return new PersistenceFetcher(persistence, "data", element.getName());
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
                .type(GraphQLTypeReference.typeRef(element.getName()));
    }

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

    private GraphQLFieldDefinition.Builder buildReferenceField(SpecificationElement property) {

        GraphQLFieldDefinition.Builder field = createFieldDefinition(property);

        GraphQLOutputType graphQLOutputType = buildReferencedField(property);
        String propertyName = property.getName();
        JsonType propertyType = elementJsonType(property);
        switch (propertyType) {
            case ARRAY:
                field.type(GraphQLList.list(graphQLOutputType));
                field.dataFetcher(new PersistenceLinksFetcher(
                        persistence,
                        "data",
                        propertyName, graphQLOutputType.getName()
                ));
                return field;
            case STRING:
                field.type(graphQLOutputType);
                field.dataFetcher(new PersistenceLinkFetcher(
                        persistence,
                        "data",
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

    private GraphQLOutputType buildReferencedField(SpecificationElement property) {
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

    private GraphQLFieldDefinition.Builder buildEmbeddedField(SpecificationElement property) {

        GraphQLFieldDefinition.Builder field = createFieldDefinition(property);

        JsonType jsonType = elementJsonType(property);
        switch (jsonType) {
            case OBJECT:
                // Recurse if embedded.
                return field.type(createObjectType(property));
            case ARRAY:
                // TODO: Extract to method.
                SpecificationElement arrayElement = property.getItems();
                JsonType arrayType = elementJsonType(arrayElement);
                if (arrayType == JsonType.OBJECT && !"".equals(arrayElement.getName())) {
                    String refType = arrayElement.getName();
                    field.type(GraphQLList.list(GraphQLTypeReference.typeRef(refType)));
                } else {
                    // TODO: Not so sure about this.
                    field.type(GraphQLList.list(GraphQLString));
                }
                return field;
            case STRING:
                return field.type(GraphQLString);
            case NUMBER:
                return field.type(GraphQLFloat);
            case BOOLEAN:
                field.type(GraphQLList.list(GraphQLBoolean));
            case INTEGER:
                return field.type(GraphQLLong);
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
