package no.ssb.lds.graphql.schemas;

import graphql.schema.GraphQLArgument;
import graphql.schema.GraphQLDirective;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLList;
import graphql.schema.GraphQLNonNull;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLType;
import graphql.schema.GraphQLTypeReference;
import graphql.schema.GraphQLTypeVisitorStub;
import graphql.util.TraversalControl;
import graphql.util.TraverserContext;

import java.util.HashSet;
import java.util.Set;

import static graphql.Scalars.GraphQLBoolean;
import static graphql.Scalars.GraphQLID;
import static graphql.Scalars.GraphQLInt;
import static graphql.Scalars.GraphQLString;

/**
 * A visitor that adds root fields to a Query definition
 * <p>
 * The visitor uses the object types marked with the @link annotations.
 */
public class GraphQLQueryBuildingVisitor extends GraphQLTypeVisitorStub {

    private final GraphQLObjectType.Builder query;
    private final Set<String> types = new HashSet<>();

    public GraphQLQueryBuildingVisitor() {
        query = GraphQLObjectType.newObject();
    }

    public GraphQLQueryBuildingVisitor(GraphQLObjectType originalQuery) {
        query = GraphQLObjectType.newObject(originalQuery);
    }

    private static boolean hasLinkDirective(GraphQLObjectType node) {
        for (GraphQLDirective directive : node.getDirectives()) {
            if ("domain".equals(directive.getName())) {
                return true;
            }
        }
        return false;
    }

    private static GraphQLFieldDefinition createUnaryField(GraphQLObjectType type) {
        return GraphQLFieldDefinition.newFieldDefinition()
                .name(type.getName() + "ById")
                .argument(
                        GraphQLArgument.newArgument()
                                .name("id")
                                .type(new GraphQLNonNull(GraphQLID))
                                .build()
                )
                .type(GraphQLNonNull.nonNull(type)).build();
    }

    public GraphQLObjectType getQuery() {
        return query.name("Query").build();
    }

    private GraphQLFieldDefinition createConnectionField(GraphQLObjectType type) {

        GraphQLFieldDefinition.Builder pageInfoField = GraphQLFieldDefinition.newFieldDefinition()
                .name("pageInfo").type(GraphQLNonNull.nonNull(createPageInfoType()));

        // Create root connection
        GraphQLFieldDefinition.Builder cursorField = GraphQLFieldDefinition.newFieldDefinition()
                .type(GraphQLNonNull.nonNull(GraphQLString)).name("cursor");

        GraphQLFieldDefinition.Builder nodeField = GraphQLFieldDefinition.newFieldDefinition()
                .type(GraphQLNonNull.nonNull(type)).name("node");

        GraphQLObjectType.Builder edgeType = GraphQLObjectType.newObject()
                .name(type.getName() + "Edge")
                .field(cursorField)
                .field(nodeField);

        GraphQLFieldDefinition.Builder edgesField = GraphQLFieldDefinition.newFieldDefinition()
                .name("edges")
                .type(GraphQLNonNull.nonNull(GraphQLList.list(GraphQLNonNull.nonNull(edgeType.build()))));


        GraphQLObjectType.Builder connectionType = GraphQLObjectType.newObject()
                .name(type.getName() + "Connection")
                .field(edgesField)
                .field(pageInfoField);
        return GraphQLFieldDefinition.newFieldDefinition()
                .name(type.getName())
                .argument(GraphQLArgument.newArgument().name("first").type(GraphQLInt).build())
                .argument(GraphQLArgument.newArgument().name("after").type(GraphQLString).build())
                .argument(GraphQLArgument.newArgument().name("last").type(GraphQLInt).build())
                .argument(GraphQLArgument.newArgument().name("before").type(GraphQLString).build())
                .type(connectionType.build())
                .build();
    }

    private GraphQLType createPageInfoType() {
        if (types.contains("PageInfo")) {
            return GraphQLTypeReference.typeRef("PageInfo");
        } else {
            types.add("PageInfo");
            return GraphQLObjectType.newObject()
                    .name("PageInfo")
                    .field(GraphQLFieldDefinition.newFieldDefinition().name("hasNextPage").type(GraphQLNonNull.nonNull(GraphQLBoolean)))
                    .field(GraphQLFieldDefinition.newFieldDefinition().name("hasPreviousPage").type(GraphQLNonNull.nonNull(GraphQLBoolean)))
                    .build();
        }
    }

    @Override
    public TraversalControl visitGraphQLObjectType(GraphQLObjectType node, TraverserContext<GraphQLType> context) {
        if (hasLinkDirective(node)) {
            query.field(createUnaryField(node));
            query.field(createConnectionField(node));
        }
        return TraversalControl.CONTINUE;
    }
}
