package no.ssb.lds.graphql.schemas;

import graphql.schema.GraphQLArgument;
import graphql.schema.GraphQLDirective;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLType;
import graphql.schema.GraphQLTypeVisitorStub;
import graphql.util.TraversalControl;
import graphql.util.TraverserContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.Objects;

import static graphql.Scalars.GraphQLString;

public class FakeAnnotationVisitor extends GraphQLTypeVisitorStub {

    private static final Logger log = LoggerFactory.getLogger(FakeAnnotationVisitor.class);
    private final Map<String, GraphQLType> typeMap;

    public FakeAnnotationVisitor(Map<String, GraphQLType> typeMap) {
        this.typeMap = Objects.requireNonNull(typeMap);
        log.warn("Using FakeAnnotationVisitor");
    }

    @Override
    public TraversalControl visitGraphQLDirective(GraphQLDirective node, TraverserContext<GraphQLType> context) {
        if (node.getName().equals("link")) {
            Deque<GraphQLType> parentNodes = new ArrayDeque<>(context.getParentNodes());
            GraphQLType field = parentNodes.removeFirst();
            GraphQLType source = parentNodes.removeFirst();
            if (field.getName().equals("representedVariable") && source.getName().equals("InstanceVariable")) {
                GraphQLObjectType.Builder newObject = GraphQLObjectType.newObject((GraphQLObjectType) source);
                GraphQLFieldDefinition.Builder newFieldDefinition = GraphQLFieldDefinition.newFieldDefinition(
                        (GraphQLFieldDefinition) field);
                GraphQLDirective.Builder newDirective = GraphQLDirective.newDirective(node);
                newDirective.argument(GraphQLArgument.newArgument()
                        .name("reverseName")
                        .type(GraphQLString)
                        .value("instanceVariables")
                        .build());
                newFieldDefinition.withDirective(newDirective);
                newObject.field(newFieldDefinition);

                typeMap.replace(source.getName(), newObject.build());
                return TraversalControl.QUIT;
            }
        }
        return TraversalControl.CONTINUE;
    }
}
