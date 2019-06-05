package no.ssb.lds.graphql.schemas;

import graphql.language.Description;
import graphql.language.Directive;
import graphql.language.DirectiveDefinition;
import graphql.language.DirectiveLocation;
import graphql.language.FieldDefinition;
import graphql.language.ListType;
import graphql.language.NonNullType;
import graphql.language.ObjectTypeDefinition;
import graphql.language.Type;
import graphql.language.TypeName;
import graphql.language.UnionTypeDefinition;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.idl.TypeDefinitionRegistry;
import no.ssb.lds.api.specification.Specification;
import no.ssb.lds.api.specification.SpecificationElement;
import no.ssb.lds.api.specification.SpecificationElementType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import static java.lang.String.format;
import static no.ssb.lds.api.specification.SpecificationElementType.MANAGED;

/**
 * This class converts a Specification to a TypeDefinitionRegistry
 * <p>
 * This is a temporary step that we do in order to stop relying on the specification.
 */
public class SpecificationConverter {

    private static final Logger log = LoggerFactory.getLogger(SpecificationConverter.class);

    private TypeDefinitionRegistry registry;

    private static Description createDescription(String description) {
        return new Description(description, null, false);
    }

    /**
     * Returns type safe element types.
     */
    static Set<JsonType> elementJsonTypes(SpecificationElement element) {
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
    public static JsonType elementJsonType(SpecificationElement element) {
        Set<JsonType> types = elementJsonTypes(element);
        types.remove(JsonType.NULL);
        if (types.size() != 1) {
            throw new IllegalArgumentException(format(
                    "more than one json type in %s: %s", element.getName(), types
            ));
        }
        return types.iterator().next();
    }

    public TypeDefinitionRegistry convert(Specification specification) {

        log.debug("Converting specification {} to GraphQL IDL", specification);

        registry = new TypeDefinitionRegistry();

        registry.add(DirectiveDefinition.newDirectiveDefinition()
                .name("domain")
                .directiveLocation(new DirectiveLocation("OBJECT"))
                .build()
        );
        registry.add(DirectiveDefinition.newDirectiveDefinition()
                .name("link")
                .directiveLocation(new DirectiveLocation("FIELD_DEFINITION"))
                .build()
        );

        Map<String, SpecificationElement> rootElements = specification.getRootElement().getProperties();
        for (String elementName : rootElements.keySet()) {
            SpecificationElement domainSpecification = rootElements.get(elementName);
            registry.add(createRootObject(domainSpecification));
        }
        registry.add(new ObjectTypeDefinition("Query"));
        return registry;
    }

    private ObjectTypeDefinition createRootObject(SpecificationElement specification) {
        log.debug("Converting root definition for {}", specification.getName());
        ObjectTypeDefinition.Builder builder = createGraphQLObject(specification);
        // getManagedDomains() in tests contains embedded objects..
        if (specification.getSpecificationElementType().equals(MANAGED)) {
            builder.directive(new Directive("domain"));
        }
        ObjectTypeDefinition objectTypeDefinition = builder.build();
        registry.add(objectTypeDefinition);
        return objectTypeDefinition;
    }

    private Type createEmbeddedObjectType(SpecificationElement specification) {
        log.debug("Creating embedded object definition for {} ", specification.getName());
        ObjectTypeDefinition objectTypeDefinition = createGraphQLObject(specification).build();
        registry.add(objectTypeDefinition);
        return new TypeName(objectTypeDefinition.getName());
    }

    private ObjectTypeDefinition.Builder createGraphQLObject(SpecificationElement specification) {
        log.debug("Creating object definition for {}", specification.getName());

        ObjectTypeDefinition.Builder builder = ObjectTypeDefinition.newObjectTypeDefinition();

        builder.name(specification.getName());
        builder.description(
                createDescription(specification.getDescription())
        );

        Map<String, SpecificationElement> properties = specification.getProperties();
        for (String propertyName : properties.keySet()) {
            SpecificationElement propertySpecification = properties.get(propertyName);
            FieldDefinition fieldDefinition = createFieldDefinition(propertySpecification);
            builder.fieldDefinition(fieldDefinition);
        }
        return builder;
    }

    private Type createReferenceType(SpecificationElement property) {

        Type type;

        // If more than one type in ref, try to create a Union type.
        Set<String> linkTypes = property.getRefTypes();
        if (linkTypes.size() > 1) {
            log.debug("Creating a union of types {}", linkTypes);
            UnionTypeDefinition.Builder union = UnionTypeDefinition.newUnionTypeDefinition();
            for (String linkType : linkTypes) {
                union.memberType(new TypeName(linkType));
            }
            union.name(property.getName());
            registry.add(union.build());
            type = new TypeName(property.getName());
        } else {
            type = new TypeName(linkTypes.iterator().next());
        }

        log.debug("Creating type reference {} for {}", property.getName(), type);

        if (elementJsonType(property) == JsonType.ARRAY) {
            type = new ListType(type);
        }
        return type;
    }

    /**
     * Helper method that create a {@link GraphQLFieldDefinition.Builder} with name and description.
     */
    private FieldDefinition createFieldDefinition(SpecificationElement property) {
        log.debug("Creating field definition for {}", property.getName());

        FieldDefinition.Builder field = FieldDefinition.newFieldDefinition();
        field.name(property.getName());
        field.description(createDescription(property.getDescription()));

        SpecificationElementType specificationType = property.getSpecificationElementType();
        Type fieldType;
        switch (specificationType) {
            case EMBEDDED:
                fieldType = createEmbeddedType(property);
                break;
            case REF:
                field.directive(new Directive("link"));
                fieldType = createReferenceType(property);
                break;
            case ROOT:
            case MANAGED:
            default:
                throw new IllegalArgumentException(format(
                        "property %s was of type %s",
                        property.getName(), specificationType
                ));
        }

        if (property.isRequired()) {
            fieldType = new NonNullType(fieldType);
        }

        return field.type(fieldType).build();
    }

    private Type createEmbeddedType(SpecificationElement property) {
        JsonType jsonType = elementJsonType(property);

        log.debug("Creating embedded type for for {} ({})", property.getName(), jsonType);

        switch (jsonType) {
            case OBJECT:
                return createEmbeddedObjectType(property);
            case ARRAY:
                return new ListType(createEmbeddedType(property.getItems()));
            case STRING:
                return new TypeName("String");
            case NUMBER:
                return new TypeName("Float");
            case BOOLEAN:
                return new TypeName("Boolean");
            case INTEGER:
                return new TypeName("Long");
        }
        throw new AssertionError("unknown type" + jsonType);
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
