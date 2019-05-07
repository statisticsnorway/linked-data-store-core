package no.ssb.lds.graphql.schemas;

import graphql.introspection.Introspection;
import graphql.schema.GraphQLArgument;
import graphql.schema.GraphQLDirective;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLList;
import graphql.schema.GraphQLNonNull;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLOutputType;
import graphql.schema.GraphQLType;
import graphql.schema.GraphQLTypeReference;
import graphql.schema.GraphQLUnionType;
import no.ssb.lds.api.specification.Specification;
import no.ssb.lds.api.specification.SpecificationElement;
import no.ssb.lds.api.specification.SpecificationElementType;
import no.ssb.lds.graphql.directives.LinkDirective;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static graphql.Scalars.GraphQLBoolean;
import static graphql.Scalars.GraphQLFloat;
import static graphql.Scalars.GraphQLLong;
import static graphql.Scalars.GraphQLString;
import static java.lang.String.format;
import static no.ssb.lds.graphql.directives.DomainDirective.newDomainDirective;
import static no.ssb.lds.graphql.schemas.OldGraphqlSchemaBuilder.JsonType;
import static no.ssb.lds.graphql.schemas.OldGraphqlSchemaBuilder.elementJsonType;
import static no.ssb.lds.graphql.schemas.OldGraphqlSchemaBuilder.getOneRefType;
import static no.ssb.lds.graphql.schemas.OldGraphqlSchemaBuilder.isNullable;

/**
 * Traverse a specification and return converted GraphQL types.
 */
public class SpecificationTraverser {

    private static final Logger log = LoggerFactory.getLogger(SpecificationTraverser.class);

    public final GraphQLDirective DOMAIN_DIRECTIVE = GraphQLDirective.newDirective()
            .name("domain")
            .argument(GraphQLArgument.newArgument().name("searchable").defaultValue(true).type(GraphQLBoolean).build())
            .validLocations(
                    Introspection.DirectiveLocation.OBJECT
            )
            .build();

    private final Specification specification;
    private Set<String> unionTypes = new HashSet<>();


    public SpecificationTraverser(Specification specification) {
        this.specification = Objects.requireNonNull(specification);
    }

    public Collection<GraphQLType> getGraphQLTypes() {
        Set<GraphQLType> domains = new LinkedHashSet<>();

        Map<String, SpecificationElement> rootElements = specification.getRootElement().getProperties();
        Set<String> managedDomains = specification.getManagedDomains();
        for (String elementName : rootElements.keySet()) {
            SpecificationElement domainSpecification = rootElements.get(elementName);
            GraphQLObjectType.Builder domainObject = createGraphQLObject(domainSpecification);
            if (managedDomains.contains(elementName)) {
                domainObject.withDirective(newDomainDirective(true));
            }
            domains.add(domainObject.build());
        }

        return domains;
    }

    private GraphQLObjectType.Builder createGraphQLObject(SpecificationElement specification) {

        GraphQLObjectType.Builder objectBuilder = GraphQLObjectType.newObject();

        objectBuilder.name(specification.getName());
        objectBuilder.description(specification.getDescription());

        Map<String, SpecificationElement> properties = specification.getProperties();
        for (String propertyName : properties.keySet()) {
            SpecificationElement propertySpecification = properties.get(propertyName);
            GraphQLFieldDefinition.Builder fieldBuilder = createFieldDefinition(propertySpecification);
            objectBuilder.field(fieldBuilder);
        }
        return objectBuilder;
    }

    /**
     * Helper method that create a {@link GraphQLFieldDefinition.Builder} with name and description.
     */
    private GraphQLFieldDefinition.Builder createFieldDefinition(SpecificationElement property) {

        GraphQLFieldDefinition.Builder field = GraphQLFieldDefinition.newFieldDefinition();
        field.name(property.getName());
        field.description(property.getDescription());

        SpecificationElementType elementType = property.getSpecificationElementType();
        GraphQLOutputType fieldType;
        switch (elementType) {
            case EMBEDDED:
                fieldType = createEmbeddedType(property);
                break;
            case REF:
                fieldType = createReferenceType(property);
                field.withDirective(LinkDirective.newLinkDirective(true));
                break;
            case ROOT:
            case MANAGED:
            default:
                throw new IllegalArgumentException(format(
                        "property %s was of type %s",
                        property.getName(), elementType
                ));
        }

        return field.type(isNullable(property) ? fieldType : GraphQLNonNull.nonNull(fieldType));
    }

    private GraphQLOutputType createEmbeddedType(SpecificationElement property) {
        JsonType jsonType = elementJsonType(property);
        switch (jsonType) {
            case OBJECT:
                return createGraphQLObject(property).build();
            case ARRAY:
                // TODO: Ideally we should recurse.
                SpecificationElement arrayElement = property.getItems();
                JsonType arrayType = elementJsonType(arrayElement);
                if (arrayType == JsonType.OBJECT && !"".equals(arrayElement.getName())) {
                    String refType = arrayElement.getName();
                    return GraphQLList.list(GraphQLNonNull.nonNull(GraphQLTypeReference.typeRef(refType)));
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

    private GraphQLOutputType createReferenceType(SpecificationElement property) {
        String propertyName = property.getName();
        JsonType propertyType = elementJsonType(property);

        GraphQLOutputType referencedType;
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
                unionTypes.add(propertyName);
                referencedType = unionType.build();
            }
        } else {
            String refType = getOneRefType(property);
            referencedType = GraphQLTypeReference.typeRef(refType);
        }

        switch (propertyType) {
            case ARRAY:
                return GraphQLList.list(referencedType);
            case STRING:
                return referencedType;
            default:
                throw new IllegalArgumentException(format(
                        "reference %s was of type %s",
                        propertyName, propertyType
                ));
        }
    }

}
