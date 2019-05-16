package no.ssb.lds.graphql.schemas.visitors;

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
import no.ssb.lds.api.json.JsonNavigationPath;
import no.ssb.lds.graphql.directives.LinkDirective;
import no.ssb.lds.graphql.directives.ReverseLinkDirective;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static no.ssb.lds.graphql.directives.DomainDirective.hasDomainDirective;
import static no.ssb.lds.graphql.directives.LinkDirective.NAME;
import static no.ssb.lds.graphql.directives.LinkDirective.newLinkDirective;

/**
 * Update the target of a link annotation with a corresponding reverseLink annotation.
 * <p>
 * The reverseLink contains a mappedBy attribute that is the path to the field of the link annotation.
 */
public class ReverseLinkBuildingVisitor extends GraphQLTypeVisitorStub {

    private static final Logger log = LoggerFactory.getLogger(ReverseLinkBuildingVisitor.class);
    private final Map<String, GraphQLType> typeMap;

    public ReverseLinkBuildingVisitor(Map<String, GraphQLType> typeMap) {
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

    private Collection<String> computePath(GraphQLFieldDefinition node, TraverserContext<GraphQLType> context) {
        List<GraphQLType> types = new ArrayList<>();
        types.add(node.getType());
        types.add(node);
        types.addAll(context.getParentNodes());
        Deque<String> parts = new ArrayDeque<>();
        for (GraphQLType type : types) {
            if (type instanceof GraphQLNonNull) {
                type = ((GraphQLNonNull) type).getWrappedType();
            }
            if (type instanceof GraphQLList) {
                parts.addFirst("[]");
            }
            if (type instanceof GraphQLFieldDefinition) {
                parts.addFirst(type.getName());
            }
            if (type instanceof GraphQLObjectType) {
                if (hasDomainDirective((GraphQLObjectType) type)) {
                    break;
                }
            }
        }
        parts.addFirst("$");
        return parts;
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

        Collection<String> parts = computePath(node, context);
        JsonNavigationPath mappedBy = JsonNavigationPath.from(parts);

        // Annotate reverse link
        GraphQLFieldDefinition fieldDefinition = GraphQLFieldDefinition.newFieldDefinition()
                .name(reverseName)
                .type(sourceType)
                .withDirective(ReverseLinkDirective.newReverseLinkDirective(true, mappedBy.serialize()))
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
            } else if (directive.getName().equals(NAME)) {
                return Optional.of(newLinkDirective(directive));
            }
        }
        return Optional.empty();
    }
}
