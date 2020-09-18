package no.ssb.lds.graphql.schemas.visitors;

import graphql.schema.GraphQLArgument;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLList;
import graphql.schema.GraphQLNonNull;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLSchemaElement;
import graphql.schema.GraphQLTypeVisitorStub;
import graphql.util.TraversalControl;
import graphql.util.TraverserContext;
import no.ssb.lds.graphql.directives.LinkDirective;

import static graphql.Scalars.GraphQLID;
import static no.ssb.lds.graphql.directives.DomainDirective.hasDomainDirective;

/**
 * A visitor that adds root fields to a Query definition
 * <p>
 * The visitor uses the object types marked with the @domain annotations.
 */
public class QueryBuildingVisitor extends GraphQLTypeVisitorStub {

    private final GraphQLObjectType.Builder query;

    public QueryBuildingVisitor(GraphQLObjectType.Builder originalQuery) {
        query = originalQuery;
    }

    public QueryBuildingVisitor(GraphQLObjectType originalQuery) {
        this(GraphQLObjectType.newObject(originalQuery));
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
                .withDirective(LinkDirective.newLinkDirective(false))
                .type(GraphQLNonNull.nonNull(type)).build();
    }

    public GraphQLObjectType getQuery() {
        return query.build();
    }

    private GraphQLFieldDefinition createNaryField(GraphQLObjectType type) {

        return GraphQLFieldDefinition.newFieldDefinition()
                .name(type.getName())
                .withDirective(LinkDirective.newLinkDirective(true))
                .type(GraphQLNonNull.nonNull(GraphQLList.list(GraphQLNonNull.nonNull(type))))
                .build();
    }


    @Override
    public TraversalControl visitGraphQLObjectType(GraphQLObjectType node, TraverserContext<GraphQLSchemaElement> context) {
        if (hasDomainDirective(node)) {
            query.field(createUnaryField(node));
            query.field(createNaryField(node));
        }
        return TraversalControl.CONTINUE;
    }
}
