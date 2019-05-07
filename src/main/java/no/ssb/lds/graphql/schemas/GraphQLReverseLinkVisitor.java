package no.ssb.lds.graphql.schemas;

import graphql.schema.GraphQLDirective;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLList;
import graphql.schema.GraphQLNonNull;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLOutputType;
import graphql.schema.GraphQLType;
import graphql.schema.GraphQLTypeReference;
import graphql.schema.GraphQLTypeUtil;
import graphql.schema.GraphQLTypeVisitorStub;
import graphql.util.TraversalControl;
import graphql.util.TraverserContext;
import no.ssb.lds.graphql.directives.LinkDirective;
import no.ssb.lds.graphql.directives.ReverseLinkDirective;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Update the target of a link annotation with reverseLink.
 */
public class GraphQLReverseLinkVisitor extends GraphQLTypeVisitorStub {

    private static final Logger log = LoggerFactory.getLogger(GraphQLReverseLinkVisitor.class);
    private final Map<String, GraphQLType> typeMap;

    public GraphQLReverseLinkVisitor(Map<String, GraphQLType> typeMap) {
        this.typeMap = Objects.requireNonNull(typeMap);
    }

    @Override
    public TraversalControl visitGraphQLFieldDefinition(GraphQLFieldDefinition node, TraverserContext<GraphQLType> context) {
        Optional<LinkDirective> linkDirective = getLinkDirective(node);
        if (linkDirective.isPresent()) {
            log.debug("Found @link annotation on {}", node.getName());
            Optional<String> reverseName = linkDirective.get().getReverseName();
            if (reverseName.isPresent()) {
                addReverseField(node, context, reverseName.get());
            }
        } else if (log.isDebugEnabled()) {
            log.trace("Ignoring field {} of {}", node.getName(), context.getParentContext().thisNode().getName());
        }

        return super.visitGraphQLFieldDefinition(node, context);
    }

    private GraphQLOutputType getObjectType(String name) {
        GraphQLType graphQLType = typeMap.get(name);
        if (graphQLType == null) {
            throw new IllegalArgumentException(String.format("Could not find the type %s", name));
        }
        try {
            return (GraphQLOutputType) graphQLType;
        } catch (ClassCastException cce) {
            log.error("The type {} was not a GraphQLObjectType", name, cce);
            throw cce;
        }
    }

    private boolean addReverseField(GraphQLFieldDefinition node, TraverserContext<GraphQLType> context, String reverseName) {
        String sourceName = GraphQLTypeUtil.unwrapAll(context.getParentNode()).getName();
        String targetName = GraphQLTypeUtil.unwrapType(node.getType()).peek().getName();

        GraphQLOutputType source = getObjectType(sourceName);
        GraphQLOutputType target = getObjectType(targetName);

        // Copy the definition
        GraphQLObjectType.Builder nodeCopy = GraphQLObjectType.newObject((GraphQLObjectType) target);

        // Add reverse link
        GraphQLOutputType sourceType = GraphQLNonNull.nonNull(
                GraphQLList.list(GraphQLNonNull.nonNull(GraphQLTypeReference.typeRef(source.getName()))));

        // Annotate reverse link
        GraphQLFieldDefinition fieldDefinition = GraphQLFieldDefinition.newFieldDefinition()
                .name(reverseName)
                .type(sourceType)
                .withDirective(ReverseLinkDirective.newReverseLinkDirective(true, node.getName()))
                .build();

        log.debug("Adding reverseLink {} from target {} to source {}", fieldDefinition, targetName, sourceName);
        nodeCopy.field(fieldDefinition);

        return typeMap.replace(target.getName(), target, nodeCopy.build());
    }

    /**
     * Checks that the node has a link directive.
     */
    private Optional<LinkDirective> getLinkDirective(GraphQLFieldDefinition node) {
        for (GraphQLDirective directive : node.getDirectives()) {
            if (directive instanceof LinkDirective) {
                return Optional.of((LinkDirective) directive);
            }
        }
        return Optional.empty();
    }
}
