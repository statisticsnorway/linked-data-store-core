package no.ssb.lds.graphql.schemas.visitors;

import graphql.schema.GraphQLArgument;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLNamedType;
import graphql.schema.GraphQLNonNull;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLSchemaElement;
import graphql.schema.GraphQLType;
import graphql.schema.GraphQLTypeVisitorStub;
import graphql.util.TraversalControl;
import graphql.util.TraverserContext;
import no.ssb.lds.graphql.directives.LinkDirective;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static graphql.schema.GraphQLDirective.newDirective;
import static graphql.schema.GraphQLFieldDefinition.newFieldDefinition;
import static graphql.schema.GraphQLObjectType.newObject;
import static no.ssb.lds.graphql.directives.DomainDirective.hasDomainDirective;
import static no.ssb.lds.graphql.directives.LinkDirective.REVERSE_NAME_ARGUMENT;
import static no.ssb.lds.graphql.schemas.visitors.ReverseLinkBuildingVisitor.getLinkDirective;

/**
 * Automatically add reverseName to the link directives
 * <p>
 * Reverse names are based on the source domain type name and the property
 * that is used as a link.
 */
public class ReverseLinkNameVisitor extends GraphQLTypeVisitorStub {

    private static final Logger log = LoggerFactory.getLogger(ReverseLinkNameVisitor.class);
    private static final String REVERSE_PREFIX = "reverse";
    private final Map<String, GraphQLNamedType> typeMap;

    public ReverseLinkNameVisitor(Map<String, GraphQLNamedType> typeMap) {
        this.typeMap = Objects.requireNonNull(typeMap);
    }

    @Override
    public TraversalControl visitGraphQLFieldDefinition(GraphQLFieldDefinition node, TraverserContext<GraphQLSchemaElement> context) {
        // Check that a link exists.
        Optional<LinkDirective> linkDirective = getLinkDirective(node);
        if (linkDirective.isPresent()) {
            return visitField(linkDirective.get(), node, context);
        }
        return TraversalControl.CONTINUE;
    }

    private Optional<GraphQLObjectType> firstObject(TraverserContext<GraphQLSchemaElement> context) {
        for (GraphQLSchemaElement parentNode : context.getParentNodes()) {
            if (parentNode instanceof GraphQLObjectType) {
                return Optional.of((GraphQLObjectType) parentNode);
            }
        }
        return Optional.empty();
    }

    private String computeName(GraphQLFieldDefinition node, TraverserContext<GraphQLSchemaElement> context) {
        List<GraphQLSchemaElement> types = new ArrayList<>();
        types.add(node);
        List<GraphQLSchemaElement> parentNodes = context.getParentNodes();
        types.addAll(parentNodes);
        Deque<String> parts = new ArrayDeque<>();
        for (GraphQLSchemaElement type : types) {
            if (type instanceof GraphQLNonNull) {
                type = ((GraphQLNonNull) type).getWrappedType();
            }
            if (type instanceof GraphQLFieldDefinition) {
                parts.addFirst(((GraphQLFieldDefinition) type).getName());
            }
            if (type instanceof GraphQLObjectType) {
                if (hasDomainDirective((GraphQLObjectType) type)) {
                    parts.addFirst(((GraphQLObjectType) type).getName());
                    break;
                }
            }
        }
        return parts.stream().map(name -> {
            if (!name.isEmpty()) {
                return Character.toUpperCase(name.charAt(0)) + name.substring(1);
            } else {
                return name;
            }
        }).collect(Collectors.joining());
    }

    private TraversalControl visitField(LinkDirective linkDirective, GraphQLFieldDefinition node, TraverserContext<GraphQLSchemaElement> context) {
        Optional<String> reverseName = linkDirective.getReverseName();
        if (reverseName.isPresent()) {
            return TraversalControl.CONTINUE;
        }

        Optional<GraphQLObjectType> objectToReplace = firstObject(context);
        if (objectToReplace.isPresent()) {
            GraphQLObjectType existing = objectToReplace.get();

            GraphQLArgument reverseNameArgument = REVERSE_NAME_ARGUMENT.transform(
                    builder -> builder.value(REVERSE_PREFIX + computeName(node, context)));

            GraphQLObjectType newObject = newObject((GraphQLObjectType) typeMap.get(existing.getName())).field(
                    newFieldDefinition(node).withDirective(
                            newDirective(linkDirective).argument(reverseNameArgument)
                    )).build();

            // TODO: Figure out why this fails. We should have tree with same instance of all types
            //if (!typeMap.replace(existing.getName(), existing, newObject)) {
            //    throw new IllegalArgumentException(String.format(
            //            "Could not replace %s, the schema probably contains references", existing.getName()
            //    ));
            //}
            GraphQLType oldObject = typeMap.put(existing.getName(), newObject);
            if (oldObject != null && Objects.equals(oldObject, existing)) {
                log.debug("Existing object {} is not equal to visited object {}", existing, oldObject);
            }
        }
        return TraversalControl.CONTINUE;
    }
}
