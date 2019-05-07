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
import graphql.schema.GraphQLTypeCollectingVisitor;
import graphql.schema.GraphQLTypeReference;
import graphql.schema.GraphQLTypeResolvingVisitor;
import graphql.schema.GraphQLTypeUtil;
import graphql.schema.GraphQLTypeVisitorStub;
import graphql.schema.GraphQLUnionType;
import graphql.schema.TypeTraverser;
import graphql.schema.idl.SchemaPrinter;
import graphql.util.TraversalControl;
import graphql.util.TraverserContext;
import no.ssb.lds.api.specification.Specification;
import no.ssb.lds.api.specification.SpecificationElement;
import no.ssb.lds.api.specification.SpecificationElementType;
import no.ssb.lds.core.specification.JsonSchemaBasedSpecification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
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
import static no.ssb.lds.graphql.schemas.GraphqlSchemaBuilder.JsonType;
import static no.ssb.lds.graphql.schemas.GraphqlSchemaBuilder.elementJsonType;
import static no.ssb.lds.graphql.schemas.GraphqlSchemaBuilder.getOneRefType;
import static no.ssb.lds.graphql.schemas.GraphqlSchemaBuilder.isNullable;

/**
 * Traverse a specification and return converted GraphQL types.
 */
public class SpecificationTraverser {

    public static final GraphQLDirective LINK_DIRECTIVE = GraphQLDirective.newDirective()
            .name("link")
            .description("Defines links between attributes and objects")
            .validLocation(Introspection.DirectiveLocation.FIELD_DEFINITION)
            .argument(GraphQLArgument.newArgument().name("pagination").type(GraphQLBoolean).defaultValue(true).build())
            .argument(GraphQLArgument.newArgument().name("reverseName").type(GraphQLString).build())
            .build();
    private static final Logger log = LoggerFactory.getLogger(SpecificationTraverser.class);
    public final GraphQLDirective DOMAIN_DIRECTIVE = GraphQLDirective.newDirective()
            .name("domain")
            .argument(GraphQLArgument.newArgument().name("searchable").defaultValue(true).type(GraphQLBoolean).build())
            .validLocations(
                    Introspection.DirectiveLocation.OBJECT
            )
            .build();
    public final GraphQLDirective REVERSE_LINK_DIRECTIVE = GraphQLDirective.newDirective()
            .name("reverseLink")
            .argument(GraphQLArgument.newArgument().name("mappedBy").type(GraphQLNonNull.nonNull(GraphQLString)).build())
            .argument(GraphQLArgument.newArgument().name("pagination").type(GraphQLBoolean).build())
            .validLocation(Introspection.DirectiveLocation.FIELD_DEFINITION)
            .build();
    private final Specification specification;
    private Set<String> unionTypes = new HashSet<>();


    public SpecificationTraverser(Specification specification) {
        this.specification = Objects.requireNonNull(specification);
    }

    /**
     * Use this to print a graphql schema out of a directory of json files.
     */
    public static void main(String... argv) {
        JsonSchemaBasedSpecification spec = JsonSchemaBasedSpecification.create(argv[0]);
        SpecificationTraverser traverser = new SpecificationTraverser(spec);
        SchemaPrinter printer = new SchemaPrinter();
        for (GraphQLType graphQLType : traverser.getGraphQLTypes()) {
            System.out.println(printer.print(graphQLType));
        }
    }

    public Collection<GraphQLType> getGraphQLTypes() {
        // First pass. Convert the SpecificationElements to a GraphQL schema with annotations.
        // the annotation are use to simplify rolling out the JsonSchema/SpecificationElement.
        Set<GraphQLObjectType> domains = new LinkedHashSet<>();

        Set<GraphQLDirective> directivesDeclaration = new HashSet<>();
        directivesDeclaration.add(LINK_DIRECTIVE);
        directivesDeclaration.add(REVERSE_LINK_DIRECTIVE);
        directivesDeclaration.add(DOMAIN_DIRECTIVE);

        Map<String, SpecificationElement> rootElements = specification.getRootElement().getProperties();
        Set<String> managedDomains = specification.getManagedDomains();
        for (String elementName : rootElements.keySet()) {
            SpecificationElement domainSpecification = rootElements.get(elementName);
            GraphQLObjectType.Builder domainObject = createGraphQLObject(domainSpecification);
            if (managedDomains.contains(elementName)) {
                domainObject.withDirective(domainDirective(true));
            }
            domains.add(domainObject.build());
        }

        if (log.isDebugEnabled()) {
            StringBuilder builder = new StringBuilder();
            SchemaPrinter printer = new SchemaPrinter();
            // Not supported yet. Coming in 13.0.
            // for (GraphQLDirective directive : directivesDeclaration) {
            //    builder.append(printer.print(directive));
            //}
            for (GraphQLType domain : domains) {
                builder.append(printer.print(domain));
            }
            log.debug("First pass schema: \n{}", builder);
        }

        GraphQLTypeCollectingVisitor graphQLTypeCollectingVisitor = new GraphQLTypeCollectingVisitor();
        new TypeTraverser().depthFirst(graphQLTypeCollectingVisitor, domains);

        Map<String, GraphQLType> typeMap = graphQLTypeCollectingVisitor.getResult();
        //GraphQLTypeResolvingVisitor typeResolvingVisitor = new GraphQLTypeResolvingVisitor(typeMap);
        //new TypeTraverser().depthFirst(typeResolvingVisitor, typeMap.values());

        // TODO: Recognize link directives and add reverse link directives.
        // TODO: Extract to own class.
        new TypeTraverser().depthFirst(new GraphQLTypeVisitorStub() {

            @Override
            public TraversalControl visitGraphQLFieldDefinition(GraphQLFieldDefinition node, TraverserContext<GraphQLType> context) {
                for (GraphQLDirective directive : node.getDirectives()) {
                    if (directive.getName().equals(LINK_DIRECTIVE.getName())) {

                        GraphQLType source = GraphQLTypeUtil.unwrapAll(context.getParentNode());
                        GraphQLType target = GraphQLTypeUtil.unwrapType(node.getType()).peek();


                        System.out.print("Source: " + source.getName());
                        System.out.println(", Target: " + target.getName());
                        return TraversalControl.CONTINUE;
                    }
                }
                return TraversalControl.CONTINUE;
            }
        }, typeMap.values());

        // TODO: Faking until link annotation is adjusted.
        Map<String, Map<String, String>> reverseMap = new HashMap<>();
        reverseMap.computeIfAbsent("RepresentedVariable", s -> new HashMap<>())
                .put("instanceVariable", "InstanceVariable");
        Map<String, Map<String, String>> linkMapMappedBy = new HashMap<>();
        linkMapMappedBy.computeIfAbsent("InstanceVariable", s -> new HashMap<>())
                .put("instanceVariable", "representedVariable");


        new TypeTraverser().depthFirst(new GraphQLTypeVisitorStub() {
            @Override
            public TraversalControl visitGraphQLObjectType(GraphQLObjectType node, TraverserContext<GraphQLType> context) {
                String typeName = node.getName();
                // Skip if has no reverse links.
                if (!reverseMap.containsKey(typeName) || reverseMap.get(typeName).isEmpty()) {
                    return TraversalControl.CONTINUE;
                }

                Map<String, String> reverseLinks = reverseMap.get(typeName);
                GraphQLObjectType.Builder nodeCopy = GraphQLObjectType.newObject(node);
                for (String reverseRelationName : reverseLinks.keySet()) {

                    String sourceTypeName = reverseLinks.get(reverseRelationName);
                    String mappedBy = linkMapMappedBy.get(sourceTypeName).get(reverseRelationName);

                    GraphQLOutputType sourceType = GraphQLNonNull.nonNull(
                            GraphQLList.list(GraphQLTypeReference.typeRef(sourceTypeName)));

                    GraphQLFieldDefinition fieldDefinition = GraphQLFieldDefinition.newFieldDefinition()
                            .name(reverseRelationName)
                            .type(sourceType)
                            .withDirective(reverseLinkDirective(mappedBy))
                            .build();

                    nodeCopy.field(fieldDefinition);
                }

                typeMap.replace(typeName, node, nodeCopy.build());

                return TraversalControl.CONTINUE;

            }
        }, typeMap.values());

        GraphQLQueryBuildingVisitor graphQLQueryVisitor = new GraphQLQueryBuildingVisitor();
        new TypeTraverser().depthFirst(graphQLQueryVisitor, typeMap.values());
        typeMap.put("Query", graphQLQueryVisitor.getQuery());

        new TypeTraverser().depthFirst(graphQLTypeCollectingVisitor, typeMap.values());

        GraphQLTypeResolvingVisitor typeResolvingVisitor = new GraphQLTypeResolvingVisitor(typeMap);
        new TypeTraverser().depthFirst(typeResolvingVisitor, typeMap.values());

        return graphQLTypeCollectingVisitor.getResult().values();
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
                field.withDirective(linkDirective(true, null));
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

    /**
     * Create a domain directive with parameter searchable set.
     */
    private GraphQLDirective domainDirective(boolean searchable) {
        return GraphQLDirective.newDirective(DOMAIN_DIRECTIVE)
                .argument(GraphQLArgument.newArgument()
                        .name("searchable")
                        .type(GraphQLBoolean)
                        .value(searchable)
                ).build();
    }

    private GraphQLDirective reverseLinkDirective(String mappedBy) {
        return GraphQLDirective.newDirective(REVERSE_LINK_DIRECTIVE)
                .argument(GraphQLArgument.newArgument()
                        .name("mappedBy")
                        .type(GraphQLString).value(mappedBy)
                ).build();
    }

    /**
     * Create a domain directive with parameter searchable set.
     */
    private GraphQLDirective linkDirective(boolean pagination, String reverseName) {
        GraphQLDirective.Builder builder = GraphQLDirective.newDirective(LINK_DIRECTIVE)
                .argument(GraphQLArgument.newArgument()
                        .name("pagination")
                        .type(GraphQLBoolean)
                        .value(pagination)
                );
        if (reverseName != null) {
            builder.argument(GraphQLArgument.newArgument()
                    .name("reverseName")
                    .type(GraphQLString)
                    .value(reverseName));
        }
        return builder.build();
    }

}
