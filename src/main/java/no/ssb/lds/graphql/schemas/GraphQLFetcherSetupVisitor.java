package no.ssb.lds.graphql.schemas;

import graphql.schema.FieldCoordinates;
import graphql.schema.GraphQLCodeRegistry;
import graphql.schema.GraphQLDirective;
import graphql.schema.GraphQLDirectiveContainer;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLList;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLOutputType;
import graphql.schema.GraphQLType;
import graphql.schema.GraphQLTypeVisitorStub;
import graphql.schema.GraphQLUnionType;
import graphql.schema.GraphQLUnmodifiedType;
import graphql.util.TraversalControl;
import graphql.util.TraverserContext;
import no.ssb.lds.api.json.JsonNavigationPath;
import no.ssb.lds.api.persistence.DocumentKey;
import no.ssb.lds.api.persistence.reactivex.RxJsonPersistence;
import no.ssb.lds.graphql.directives.LinkDirective;
import no.ssb.lds.graphql.directives.ReverseLinkDirective;
import no.ssb.lds.graphql.fetcher.PersistenceFetcher;
import no.ssb.lds.graphql.fetcher.PersistenceLinkFetcher;
import no.ssb.lds.graphql.fetcher.PersistenceLinksConnectionFetcher;
import no.ssb.lds.graphql.fetcher.PersistenceLinksFetcher;
import no.ssb.lds.graphql.fetcher.PersistenceReverseLinksConnectionFetcher;
import no.ssb.lds.graphql.fetcher.PersistenceRootConnectionFetcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.Stack;

import static graphql.schema.GraphQLTypeUtil.isList;
import static graphql.schema.GraphQLTypeUtil.isNotWrapped;
import static graphql.schema.GraphQLTypeUtil.simplePrint;
import static graphql.schema.GraphQLTypeUtil.unwrapAll;
import static graphql.schema.GraphQLTypeUtil.unwrapType;
import static no.ssb.lds.graphql.directives.DomainDirective.hasDomainDirective;

public class GraphQLFetcherSetupVisitor extends GraphQLTypeVisitorStub {

    private static final Logger log = LoggerFactory.getLogger(GraphQLFetcherSetupVisitor.class);
    private final GraphQLCodeRegistry.Builder registry;
    private final RxJsonPersistence persistence;
    private final String namespace;

    public GraphQLFetcherSetupVisitor(RxJsonPersistence persistence, String namespace) {
        this(GraphQLCodeRegistry.newCodeRegistry(), persistence, namespace);
    }

    public GraphQLFetcherSetupVisitor(GraphQLCodeRegistry registry, RxJsonPersistence persistence, String namespace) {
        this(GraphQLCodeRegistry.newCodeRegistry(registry), persistence, namespace);
    }

    public GraphQLFetcherSetupVisitor(GraphQLCodeRegistry.Builder registry, RxJsonPersistence persistence, String namespace) {
        this.registry = registry;
        this.persistence = persistence;
        this.namespace = namespace;
    }

    private static Boolean isMany(GraphQLOutputType type) {
        Stack<GraphQLType> types = unwrapType(type);
        for (GraphQLType currentType : types) {
            if (isList(currentType)) {
                return true;
            }
        }
        return false;
    }

    private static Boolean isConnection(GraphQLOutputType type) {
        Stack<GraphQLType> types = unwrapType(type);
        for (GraphQLType currentType : types) {
            if (isNotWrapped(currentType) && currentType.getName().endsWith("Connection")) {
                return true;
            }
        }
        return false;
    }

    private static Boolean hasReverseLinkDirective(GraphQLDirectiveContainer container) {
        for (GraphQLDirective directive : container.getDirectives()) {
            if (directive.getName().equals(ReverseLinkDirective.NAME)) {
                return true;
            }
        }
        return false;
    }

    private static Boolean hasLinkDirective(GraphQLDirectiveContainer container) {
        for (GraphQLDirective directive : container.getDirectives()) {
            if (directive.getName().equals(LinkDirective.NAME)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isOneToMany(GraphQLFieldDefinition field, TraverserContext<GraphQLType> context) {
        GraphQLOutputType targetType = field.getType();
        return isMany(targetType);
    }

    private static boolean isOneToOne(GraphQLFieldDefinition field, TraverserContext<GraphQLType> context) {
        return !isOneToMany(field, context);
    }

    public GraphQLCodeRegistry getRegistry() {
        return registry.build();
    }

    private TraversalControl visitLinkField(GraphQLFieldDefinition field, TraverserContext<GraphQLType> context) {
        if (isOneToMany(field, context) && !isConnection(field, context)) {
            return visitOneToManyLink(field, context);
        } else if (!isOneToMany(field, context) && isConnection(field, context)) {
            return visitConnectionLink(field, context);
        } else if (isOneToOne(field, context)) {
            return visitOneToOneLink(field, context);
        } else {
            return TraversalControl.CONTINUE;
        }
    }

    private TraversalControl visitConnectionLink(GraphQLFieldDefinition field, TraverserContext<GraphQLType> context) {
        GraphQLObjectType sourceObject = (GraphQLObjectType) context.getParentNode();
        GraphQLOutputType targetType = field.getType();

        // Unwrap the connection.
        // [Source][Target]Connection -edges-> [Source][Target]Egde -node-> Target
        GraphQLObjectType connectionType = (GraphQLObjectType) unwrapAll(targetType);
        GraphQLObjectType edgeType = (GraphQLObjectType) unwrapAll(
                connectionType.getFieldDefinition("edges").getType());
        GraphQLUnmodifiedType nodeType = unwrapAll(
                edgeType.getFieldDefinition("node").getType());

        if (sourceObject.getName().equals("Query")) {
            log.debug("RootConnection: {} -> {} -> {} ",
                    simplePrint(sourceObject),
                    field.getName(),
                    simplePrint(nodeType)
            );
            registry.dataFetcher(FieldCoordinates.coordinates(sourceObject, field),
                    new PersistenceRootConnectionFetcher(persistence, namespace, nodeType.getName()));
        } else {

            if (hasReverseLinkDirective(field)) {
                log.debug("ReverseConnection: {} -> {} -> {} ",
                        simplePrint(sourceObject),
                        field.getName(),
                        simplePrint(nodeType)
                );
                registry.dataFetcher(FieldCoordinates.coordinates(sourceObject, field),
                        new PersistenceReverseLinksConnectionFetcher(persistence, namespace, nodeType.getName(),
                                getReverseJsonNavigationPath(field, context), sourceObject.getName()));
            } else {
                log.debug("Connection: {} -> {} -> {} ",
                        simplePrint(sourceObject),
                        field.getName(),
                        simplePrint(nodeType)
                );
                registry.dataFetcher(FieldCoordinates.coordinates(sourceObject, field),
                        new PersistenceLinksConnectionFetcher(persistence, namespace, sourceObject.getName(),
                                getJsonNavigationPath(field, context), nodeType.getName()));
            }
        }
        return TraversalControl.CONTINUE;
    }

    private boolean isConnection(GraphQLFieldDefinition field, TraverserContext<GraphQLType> context) {
        GraphQLOutputType targetType = field.getType();
        return isConnection(targetType);
    }

    private JsonNavigationPath getReverseJsonNavigationPath(GraphQLFieldDefinition field, TraverserContext<GraphQLType> context) {
        String mappedBy = (String) field.getDirective(ReverseLinkDirective.NAME)
                .getArgument(ReverseLinkDirective.MAPPED_BY_NAME).getValue();
        return JsonNavigationPath.from(mappedBy);
    }

    private JsonNavigationPath getJsonNavigationPath(GraphQLFieldDefinition field, TraverserContext<GraphQLType> context) {
        Deque<String> parts = new ArrayDeque<>();
        parts.addFirst(field.getName());
        // Iterate through parents until we find a domain object.
        for (GraphQLType currentNode : context.getParentNodes()) {
            if (currentNode instanceof GraphQLList) {
                parts.addFirst("[]");
            }
            if (currentNode instanceof GraphQLFieldDefinition) {
                parts.addFirst(currentNode.getName());
            }
            if (currentNode instanceof GraphQLObjectType) {
                if (hasDomainDirective((GraphQLObjectType) currentNode)) {
                    break;
                }
            }
        }
        parts.addFirst("$");
        return JsonNavigationPath.from(parts);
    }


    private TraversalControl visitOneToManyLink(GraphQLFieldDefinition field, TraverserContext<GraphQLType> context) {
        GraphQLObjectType sourceObject = (GraphQLObjectType) context.getParentNode();
        GraphQLOutputType targetType = field.getType();
        if (sourceObject.getName().equals("Query")) {
            log.debug("RootOneToMany: {} -> {} -> {} ",
                    simplePrint(sourceObject),
                    field.getName(),
                    simplePrint(unwrapAll(targetType))
            );
            registry.dataFetcher(FieldCoordinates.coordinates(sourceObject, field),
                    new PersistenceLinkFetcher(persistence, namespace, field.getName(), unwrapAll(targetType).getName()));
        } else {
            if (hasReverseLinkDirective(field)) {
                log.debug("ManyToOne: {} -> {} -> {}",
                        simplePrint(sourceObject),
                        field.getName(),
                        simplePrint(unwrapAll(targetType))
                );
                log.warn("ManyToOne: is not supported for reverse links");
            } else {
                log.debug("OneToMany: {} -> {} -> {} ",
                        simplePrint(sourceObject),
                        field.getName(),
                        simplePrint(unwrapAll(targetType))
                );
                registry.dataFetcher(FieldCoordinates.coordinates(sourceObject, field), new PersistenceLinksFetcher(
                        persistence, namespace, field.getName(), unwrapAll(targetType).getName()));
            }
        }
        return TraversalControl.CONTINUE;
    }

    private TraversalControl visitOneToOneLink(GraphQLFieldDefinition field, TraverserContext<GraphQLType> context) {
        GraphQLObjectType sourceObject = (GraphQLObjectType) context.getParentNode();
        GraphQLOutputType targetType = field.getType();
        if (sourceObject.getName().equals("Query")) {
            log.debug("RootOneToOne: {} -> {} -> {} ",
                    simplePrint(sourceObject),
                    field.getName(),
                    simplePrint(unwrapAll(targetType))
            );
            registry.dataFetcher(FieldCoordinates.coordinates(sourceObject, field), new PersistenceFetcher(persistence,
                    namespace, unwrapAll(targetType).getName()));
        } else {
            log.debug("OneToOne: {} -> {} -> {} ",
                    simplePrint(sourceObject),
                    field.getName(),
                    simplePrint(unwrapAll(targetType))
            );
            registry.dataFetcher(FieldCoordinates.coordinates(sourceObject, field), new PersistenceLinkFetcher(
                    persistence, namespace, field.getName(), unwrapAll(targetType).getName()));
        }
        return TraversalControl.CONTINUE;
    }

    @Override
    public TraversalControl visitGraphQLUnionType(GraphQLUnionType node, TraverserContext<GraphQLType> context) {
        registry.typeResolver(node, env -> {
            Map<String, Object> object = env.getObject();
            return (GraphQLObjectType) env.getSchema().getType(((DocumentKey) object.get("__graphql_internal_document_key")).entity());
        });
        return TraversalControl.CONTINUE;
    }

    @Override
    public TraversalControl visitGraphQLFieldDefinition(GraphQLFieldDefinition field, TraverserContext<GraphQLType> context) {
        if (hasReverseLinkDirective(field) || hasLinkDirective(field)) {
            return visitLinkField(field, context);
        } else {
            return TraversalControl.CONTINUE;
        }
    }
}
