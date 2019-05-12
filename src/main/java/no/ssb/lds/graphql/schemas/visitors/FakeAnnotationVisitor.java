package no.ssb.lds.graphql.schemas.visitors;

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
import java.util.Set;

import static no.ssb.lds.graphql.directives.LinkDirective.newLinkDirective;

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
                newFieldDefinition.withDirective(newLinkDirective(true, "instanceVariables"));
                newObject.field(newFieldDefinition);

                typeMap.replace(source.getName(), newObject.build());
            }

            if (field.getName().equals("logicalRecords") && source.getName().equals("UnitDataStructure")) {
                GraphQLObjectType.Builder newObject = GraphQLObjectType.newObject((GraphQLObjectType) source);
                GraphQLFieldDefinition.Builder newFieldDefinition = GraphQLFieldDefinition.newFieldDefinition(
                        (GraphQLFieldDefinition) field);
                newFieldDefinition.withDirective(newLinkDirective(true, "unitDataStructures"));
                newObject.field(newFieldDefinition);

                typeMap.replace(source.getName(), newObject.build());
            }

            // Reverse links on one to many relations.
            if (source.getName().equals("LogicalRecord")) {
                Set<String> componentFields = Set.of("identifierComponents", "measureComponents", "attributeComponents");
                if (componentFields.contains(field.getName())) {
                    GraphQLObjectType.Builder newObject = GraphQLObjectType.newObject((GraphQLObjectType) source);
                    GraphQLFieldDefinition.Builder newFieldDefinition = GraphQLFieldDefinition.newFieldDefinition(
                            (GraphQLFieldDefinition) field);
                    newFieldDefinition.withDirective(newLinkDirective(true, "logicalRecords"));
                    newObject.field(newFieldDefinition);

                    typeMap.replace(source.getName(), newObject.build());
                }
            }

        }
        return TraversalControl.CONTINUE;
    }
}
