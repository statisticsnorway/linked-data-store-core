package no.ssb.lds.graphql.schemas;

import graphql.schema.GraphQLArgument;
import graphql.schema.GraphQLDirective;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLList;
import graphql.schema.GraphQLNonNull;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLType;
import graphql.schema.GraphQLTypeVisitorStub;
import graphql.util.TraversalControl;
import graphql.util.TraverserContext;

import static graphql.Scalars.GraphQLBoolean;
import static graphql.Scalars.GraphQLID;

/**
 * A visitor that adds root fields to a Query definition
 * <p>
 * The visitor uses the object types marked with the @domain annotations.
 */
public class GraphQLQueryBuildingVisitor extends GraphQLTypeVisitorStub {

    private final GraphQLObjectType.Builder query;

    public GraphQLQueryBuildingVisitor() {
        query = GraphQLObjectType.newObject();
    }

    public GraphQLQueryBuildingVisitor(GraphQLObjectType originalQuery) {
        query = GraphQLObjectType.newObject(originalQuery);
    }

    private static boolean hasDomainDirective(GraphQLObjectType node) {
        for (GraphQLDirective directive : node.getDirectives()) {
            if ("domain".equals(directive.getName())) {
                return true;
            }
        }
        return false;
    }

    private GraphQLDirective createLinkDirective(boolean pagination) {
        GraphQLDirective.Builder link = GraphQLDirective.newDirective()
                .name("link");
        if (!pagination) {
            link.argument(GraphQLArgument.newArgument()
                    .name("pagination")
                    .type(GraphQLBoolean)
                    .value(pagination)
                    .build());
        }
        return link.build();
    }

    private GraphQLFieldDefinition createUnaryField(GraphQLObjectType type) {
        return GraphQLFieldDefinition.newFieldDefinition()
                .name(type.getName() + "ById")
                .argument(
                        GraphQLArgument.newArgument()
                                .name("id")
                                .type(new GraphQLNonNull(GraphQLID))
                                .build()
                )
                .withDirective(createLinkDirective(false))
                .type(GraphQLNonNull.nonNull(type)).build();
    }

    public GraphQLObjectType getQuery() {
        return query.name("Query").build();
    }

    private GraphQLFieldDefinition createNaryField(GraphQLObjectType type) {

        return GraphQLFieldDefinition.newFieldDefinition()
                .name(type.getName())
                .withDirective(createLinkDirective(true))
                .type(GraphQLNonNull.nonNull(GraphQLList.list(GraphQLNonNull.nonNull(type))))
                .build();
    }


    @Override
    public TraversalControl visitGraphQLObjectType(GraphQLObjectType node, TraverserContext<GraphQLType> context) {
        if (hasDomainDirective(node)) {
            query.field(createUnaryField(node));
            query.field(createNaryField(node));
        }
        return TraversalControl.CONTINUE;
    }
}
