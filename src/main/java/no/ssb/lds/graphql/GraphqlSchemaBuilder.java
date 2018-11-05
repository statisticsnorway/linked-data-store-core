package no.ssb.lds.graphql;

import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLList;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLSchema;
import graphql.schema.GraphQLTypeReference;
import graphql.schema.idl.SchemaGenerator;
import graphql.schema.idl.TypeDefinitionRegistry;
import no.ssb.lds.core.specification.Specification;
import no.ssb.lds.core.specification.SpecificationElement;
import no.ssb.lds.core.specification.SpecificationElementType;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static graphql.Scalars.GraphQLFloat;
import static graphql.Scalars.GraphQLLong;
import static graphql.Scalars.GraphQLString;
import static no.ssb.lds.core.specification.SpecificationElementType.EMBEDDED;
import static no.ssb.lds.core.specification.SpecificationElementType.REF;

/**
 * Converts a LDS specification to GraphQL schema.
 */
public class GraphqlSchemaBuilder {

    private final Specification specification;

    public GraphqlSchemaBuilder(Specification specification) {
        this.specification = Objects.requireNonNull(specification);
    }

    private static Boolean isRequired(SpecificationElement property) {
        return property.getJsonTypes().contains("null");
    }

    private static String getOneJsonType(SpecificationElement property) {
        if (!isRequired(property)) {
            Set<String> types = property.getJsonTypes();
            if (types.size() != 1) {
                throw new IllegalArgumentException("more than one json type");
            }
            return types.iterator().next();
        } else {
            // EW!
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
            throw new IllegalArgumentException("More than one ref type?");
        }
        return types.iterator().next();
    }

    public GraphQLSchema getSchema() {
        // Build a graphql schema out of the specification.
        SpecificationElement root = specification.getRootElement();
        Map<String, GraphQLObjectType.Builder> types = new LinkedHashMap<>();
        for (SpecificationElement element : root.getProperties().values()) {
            // Should be MANAGED and type object.
            types.put(element.getName(), convert(element));
        }
        return null;
    }

    public GraphQLObjectType.Builder convert(SpecificationElement specificationElement) {
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
                    field.type(convert(property));
                } else if ("array".equals(jsonType)) {
                    // TODO
                    throw new UnsupportedOperationException("TODO: Implement array json type");
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
