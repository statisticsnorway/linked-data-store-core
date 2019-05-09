package no.ssb.lds.graphql.schemas;

import graphql.schema.GraphQLArgument;
import graphql.schema.GraphQLDirective;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLList;
import graphql.schema.GraphQLNonNull;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLOutputType;
import graphql.schema.GraphQLType;
import graphql.schema.GraphQLTypeUtil;
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

/**
 * Add a @pagination annotation to all fields marked with @link or @reverseLink.
 */
public class GraphQLPaginationVisitor extends GraphQLTypeVisitorStub {

    private static final Logger log = LoggerFactory.getLogger(GraphQLPaginationVisitor.class);
    private final Map<String, GraphQLType> typeMap;

    public GraphQLPaginationVisitor(Map<String, GraphQLType> typeMap) {
        this.typeMap = Objects.requireNonNull(typeMap);
    }

    public static boolean hasLinkWithPagination(GraphQLFieldDefinition node) {
        for (GraphQLDirective directive : node.getDirectives()) {
            if ("link".equals(directive.getName()) || "reverseLink".equals(directive.getName())) {
                GraphQLArgument pagination = directive.getArgument("pagination");
                // TODO: Figure out how arguments are supposed to be used.
                if (pagination == null) {
                    return false;
                }
                Object value = pagination.getValue();
                if (value == null) {
                    return false;
                }
                return value.equals(true);
            }
            if ("search".equals(directive.getName())) {
                return true;
            }
        }
        return false;
    }

    @Override
    public TraversalControl visitGraphQLObjectType(GraphQLObjectType node, TraverserContext<GraphQLType> context) {

        List<GraphQLFieldDefinition> fieldsWithPagination = new ArrayList<>();
        for (GraphQLFieldDefinition fieldDefinition : node.getFieldDefinitions()) {
            if (hasLinkWithPagination(fieldDefinition)) {
                log.debug("Found link from {} to {} on field {}", node.getName(), GraphQLTypeUtil.simplePrint(fieldDefinition.getType()),
                        fieldDefinition.getName());
                fieldsWithPagination.add(fieldDefinition);
            }
        }
        if (fieldsWithPagination.isEmpty()) {
            log.debug("No fields marked with pagination in {}", node);
            return TraversalControl.CONTINUE;
        }

        log.debug("Transforming {} fields to pagination fields", fieldsWithPagination.size());

        GraphQLObjectType.Builder newObject = GraphQLObjectType.newObject(node);
        for (GraphQLFieldDefinition fieldDefinition : fieldsWithPagination) {
            newObject.field(createConnectionField(node, fieldDefinition));
        }

        typeMap.put(node.getName(), newObject.build());

        return TraversalControl.CONTINUE;
    }

    private GraphQLFieldDefinition createConnectionField(GraphQLObjectType from, GraphQLFieldDefinition field) {
        GraphQLType to = GraphQLTypeUtil.unwrapType(field.getType()).pop();
        GraphQLFieldDefinition.Builder newFieldDefinition = GraphQLFieldDefinition.newFieldDefinition(field);
        return newFieldDefinition
                .argument(GraphQLArgument.newArgument().name("first").type(GraphQLInt).build())
                .argument(GraphQLArgument.newArgument().name("after").type(GraphQLString).build())
                .argument(GraphQLArgument.newArgument().name("last").type(GraphQLInt).build())
                .argument(GraphQLArgument.newArgument().name("before").type(GraphQLString).build())
                .type((GraphQLOutputType) createConnectionType(from, to))
                .build();
    }

    private GraphQLType createConnectionType(GraphQLObjectType from, GraphQLType to) {
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

    private GraphQLType createEdgeType(GraphQLType from, GraphQLType to) {
        String edgeTypeName = from.getName() + to.getName() + "Edge";
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
