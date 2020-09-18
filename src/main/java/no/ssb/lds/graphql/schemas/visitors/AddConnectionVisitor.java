package no.ssb.lds.graphql.schemas.visitors;

import graphql.schema.GraphQLArgument;
import graphql.schema.GraphQLDirective;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLFieldsContainer;
import graphql.schema.GraphQLInterfaceType;
import graphql.schema.GraphQLList;
import graphql.schema.GraphQLNamedType;
import graphql.schema.GraphQLNonNull;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLOutputType;
import graphql.schema.GraphQLSchemaElement;
import graphql.schema.GraphQLType;
import graphql.schema.GraphQLTypeVisitorStub;
import graphql.util.TraversalControl;
import graphql.util.TraverserContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static graphql.Scalars.GraphQLBoolean;
import static graphql.Scalars.GraphQLInt;
import static graphql.Scalars.GraphQLString;
import static graphql.schema.GraphQLTypeUtil.isList;
import static graphql.schema.GraphQLTypeUtil.simplePrint;
import static graphql.schema.GraphQLTypeUtil.unwrapNonNull;
import static graphql.schema.GraphQLTypeUtil.unwrapType;

/**
 * Add a @pagination annotation to all fields marked with @link or @reverseLink.
 */
public class AddConnectionVisitor extends GraphQLTypeVisitorStub {

    private static final Logger log = LoggerFactory.getLogger(AddConnectionVisitor.class);
    private final Map<String, GraphQLNamedType> typeMap;

    public AddConnectionVisitor(Map<String, GraphQLNamedType> typeMap) {
        this.typeMap = Objects.requireNonNull(typeMap);
    }

    public static boolean hasLinkWithPagination(GraphQLFieldDefinition node) {
        for (GraphQLDirective directive : node.getDirectives()) {
            if ("link".equals(directive.getName()) || "reverseLink".equals(directive.getName())) {
                GraphQLArgument pagination = directive.getArgument("pagination");
                // TODO: Figure out how arguments are supposed to be used.
                if (pagination == null) {
                    return true;
                }
                Object value = pagination.getValue();
                if (value == null) {
                    return true;
                }
                return (Boolean) value;
            }
            if ("search".equals(directive.getName())) {
                return true;
            }
        }
        return false;
    }

    @Override
    public TraversalControl visitGraphQLInterfaceType(GraphQLInterfaceType node, TraverserContext<GraphQLSchemaElement> context) {
        List<GraphQLFieldDefinition> fieldsWithPagination = findFieldsWithPagination(node);
        if (fieldsWithPagination.isEmpty()) {
            log.trace("No fields marked with pagination in {}", node);
            return TraversalControl.CONTINUE;
        }

        log.debug("Transforming {} fields to pagination fields in {}", fieldsWithPagination.size(), node.getName());

        GraphQLInterfaceType.Builder newObject = GraphQLInterfaceType.newInterface(node);
        for (GraphQLFieldDefinition fieldDefinition : fieldsWithPagination) {
            newObject.field(createConnectionField(node, fieldDefinition));
        }

        typeMap.put(node.getName(), newObject.build());

        return TraversalControl.CONTINUE;
    }

    private List<GraphQLFieldDefinition> findFieldsWithPagination(GraphQLFieldsContainer node) {
        List<GraphQLFieldDefinition> fieldsWithPagination = new ArrayList<>();
        for (GraphQLFieldDefinition fieldDefinition : node.getFieldDefinitions()) {
            if (hasLinkWithPagination(fieldDefinition) && isList(unwrapNonNull(fieldDefinition.getType()))) {
                log.trace("Found link from {} to {} on field {}", node.getName(), simplePrint(fieldDefinition.getType()),
                        fieldDefinition.getName());
                fieldsWithPagination.add(fieldDefinition);
            }
        }
        return fieldsWithPagination;
    }

    @Override
    public TraversalControl visitGraphQLObjectType(GraphQLObjectType node, TraverserContext<GraphQLSchemaElement> context) {
        List<GraphQLFieldDefinition> fieldsWithPagination = findFieldsWithPagination(node);
        if (fieldsWithPagination.isEmpty()) {
            log.trace("No fields marked with pagination in {}", node);
            return TraversalControl.CONTINUE;
        }

        log.debug("Transforming {} fields to pagination fields in {}", fieldsWithPagination.size(), node.getName());

        GraphQLObjectType.Builder newObject = GraphQLObjectType.newObject(
                (GraphQLObjectType) typeMap.get(node.getName()));
        for (GraphQLFieldDefinition fieldDefinition : fieldsWithPagination) {
            newObject.field(createConnectionField(node, fieldDefinition));
        }

        // TODO: Figure out why this fails. We should have tree with same instance of all types
        //if (!typeMap.replace(existing.getName(), existing, newObject)) {
        //    throw new IllegalArgumentException(String.format(
        //            "Could not replace %s, the schema probably contains references", existing.getName()
        //    ));
        //}
        GraphQLType oldObject = typeMap.put(node.getName(), newObject.build());
        if (oldObject != null && Objects.equals(oldObject, node)) {
            log.debug("Existing object {} is not equal to visited object {}", node, oldObject);
        }

        return TraversalControl.CONTINUE;
    }

    private GraphQLFieldDefinition createConnectionField(GraphQLNamedType from, GraphQLFieldDefinition field) {
        GraphQLNamedType to = (GraphQLNamedType) unwrapType(field.getType()).pop();
        GraphQLFieldDefinition.Builder newFieldDefinition = GraphQLFieldDefinition.newFieldDefinition(field);
        return newFieldDefinition
                .argument(GraphQLArgument.newArgument().name("first").type(GraphQLInt).build())
                .argument(GraphQLArgument.newArgument().name("after").type(GraphQLString).build())
                .argument(GraphQLArgument.newArgument().name("last").type(GraphQLInt).build())
                .argument(GraphQLArgument.newArgument().name("before").type(GraphQLString).build())
                .type((GraphQLOutputType) createConnectionType(from, to))
                .build();
    }

    private GraphQLType createConnectionType(GraphQLNamedType from, GraphQLNamedType to) {
        // We do not use Relay Connection name convertion to reduce the amount of
        // types.
        // String connectionTypeName = from.getName() + to.getName() + "Connection";
        String connectionTypeName = to.getName() + "Connection";
        if (!typeMap.containsKey(connectionTypeName)) {
            GraphQLFieldDefinition.Builder pageInfoField = GraphQLFieldDefinition.newFieldDefinition()
                    .type(GraphQLNonNull.nonNull(createPageInfoType())).name("pageInfo");

            GraphQLFieldDefinition.Builder edgesField = GraphQLFieldDefinition.newFieldDefinition()
                    .type(GraphQLNonNull.nonNull(
                            GraphQLList.list(GraphQLNonNull.nonNull(
                                    createEdgeType(from, to))
                            ))
                    )
                    .name("edges");

            GraphQLObjectType.Builder connectionType = GraphQLObjectType.newObject()
                    .name(connectionTypeName)
                    .field(edgesField)
                    .field(pageInfoField);

            typeMap.put(connectionTypeName, connectionType.build());
        }
        return typeMap.get(connectionTypeName);
    }

    private GraphQLType createEdgeType(GraphQLNamedType from, GraphQLNamedType to) {
        // We do not use Relay Connection name conversion to reduce the amount of
        // types.
        // String edgeTypeName = from.getName() + to.getName() + "Connection";
        String edgeTypeName = to.getName() + "Edge";
        if (!typeMap.containsKey(edgeTypeName)) {

            GraphQLFieldDefinition.Builder nodeField = GraphQLFieldDefinition.newFieldDefinition()
                    .type(GraphQLNonNull.nonNull(to)).name("node");

            GraphQLFieldDefinition.Builder cursorField = GraphQLFieldDefinition.newFieldDefinition()
                    .type(GraphQLNonNull.nonNull(GraphQLString)).name("cursor");

            GraphQLObjectType.Builder edgeType = GraphQLObjectType.newObject()
                    .name(edgeTypeName)
                    .field(cursorField)
                    .field(nodeField);

            typeMap.put(edgeTypeName, edgeType.build());
        }
        return typeMap.get(edgeTypeName);
    }

    private GraphQLType createPageInfoType() {
        if (!typeMap.containsKey("PageInfo")) {
            GraphQLObjectType pageInfoType = GraphQLObjectType.newObject()
                    .name("PageInfo")
                    .field(GraphQLFieldDefinition.newFieldDefinition().name("hasNextPage").type(GraphQLNonNull.nonNull(GraphQLBoolean)))
                    .field(GraphQLFieldDefinition.newFieldDefinition().name("hasPreviousPage").type(GraphQLNonNull.nonNull(GraphQLBoolean)))
                    .build();
            typeMap.put("PageInfo", pageInfoType);
        }
        return typeMap.get("PageInfo");
    }
}
