package no.ssb.lds.graphql;

import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLList;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLSchema;
import graphql.schema.GraphQLType;
import graphql.schema.GraphQLTypeReference;
import graphql.schema.StaticDataFetcher;
import no.ssb.lds.core.specification.Specification;
import no.ssb.lds.core.specification.SpecificationElement;
import no.ssb.lds.core.specification.SpecificationElementType;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

import static graphql.Scalars.GraphQLBoolean;
import static graphql.Scalars.GraphQLFloat;
import static graphql.Scalars.GraphQLLong;
import static graphql.Scalars.GraphQLString;
import static java.lang.String.*;
import static no.ssb.lds.core.specification.SpecificationElementType.EMBEDDED;
import static no.ssb.lds.core.specification.SpecificationElementType.REF;

/**
 * Converts a LDS specification to GraphQL schema.
 */
public class GraphqlSchemaBuilder {

    private static final Logger log = LoggerFactory.getLogger(GraphqlSchemaBuilder.class);
    private final Specification specification;

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

        // TODO: createAdditionaTypes();
        Set<GraphQLType> types = new LinkedHashSet<>();
        SpecificationElement root = specification.getRootElement();
        for (SpecificationElement element : root.getProperties().values()) {
            GraphQLObjectType buildType = createObjectType(element).build();
            log.debug("Converted {} to graphql type {}", element.getName(), buildType);
            types.add(buildType);
        }

        // TODO: Create query dynamically.
        //GraphQLSchema build = GraphQLSchema.newSchema().query(
        //        GraphQLObjectType.newObject()
        //                .name("Query")
        //                .field(
        //                        GraphQLFieldDefinition.newFieldDefinition()
        //                                .name("contact")
        //                                .argument(
        //                                        GraphQLArgument.newArgument()
        //                                                .name("id")
        //                                                .type(GraphQLID)
        //                                                .build()
        //                                )
        //                                .type(GraphQLList.list(GraphQLTypeReference.typeRef("contact")))
        //                                .dataFetcher(
        //                                        new StaticDataFetcher(
        //                                                Arrays.asList(
        //                                                        new JSONObject(Map.of("name", "Hadrien")).toMap(),
        //                                                        new JSONObject(Map.of("name", "Kim")).toMap()
        //                                                )
        //                                        )
        //                                )
        //                                .build()
        //                )
        //).additionalTypes(types).build();
        return GraphQLSchema.newSchema().query(GraphQLObjectType.newObject().name("Query").build()).additionalTypes(types).build();
    }

    public GraphQLObjectType.Builder createObjectType(SpecificationElement specificationElement) {
        GraphQLObjectType.Builder object = GraphQLObjectType.newObject();
        object.name(specificationElement.getName());

        // For each property
        for (SpecificationElement property : specificationElement.getProperties().values()) {

            GraphQLFieldDefinition.Builder field = GraphQLFieldDefinition.newFieldDefinition();
            field.name(property.getName());

            SpecificationElementType elementType = property.getSpecificationElementType();
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
                // TODO: are ref always arrays?
                String refType = getOneRefType(property);
                field.type(GraphQLList.list(GraphQLTypeReference.typeRef(refType)));
            } else {
                throw new AssertionError();
            }

            object.field(field);
        }

        return object;
    }

}
