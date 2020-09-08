package no.ssb.lds.graphql.fetcher;

import graphql.relay.Connection;
import graphql.relay.DefaultConnection;
import graphql.relay.DefaultPageInfo;
import graphql.relay.Edge;
import graphql.relay.PageInfo;
import graphql.schema.DataFetchingEnvironment;
import graphql.schema.GraphQLInterfaceType;
import graphql.schema.GraphQLNamedOutputType;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLOutputType;
import graphql.schema.GraphQLSchema;
import graphql.schema.GraphQLType;
import graphql.schema.GraphQLUnionType;
import io.reactivex.Flowable;
import no.ssb.lds.api.json.JsonNavigationPath;
import no.ssb.lds.api.persistence.DocumentKey;
import no.ssb.lds.api.persistence.Transaction;
import no.ssb.lds.api.persistence.json.JsonDocument;
import no.ssb.lds.api.persistence.reactivex.Range;
import no.ssb.lds.api.persistence.reactivex.RxJsonPersistence;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A fetcher that support relay style connection parameters and type.
 */
public class PersistenceLinksConnectionFetcher extends ConnectionFetcher<Map<String, Object>> {

    // Field name containing the ids of the target.
    private final JsonNavigationPath relationPath;

    private final String sourceEntityName;

    // Target entity name.
    private final String targetEntityName;

    // Name space
    private final String nameSpace;

    private final RxJsonPersistence persistence;
    public static final Comparator<JsonDocument> BY_ID = Comparator.comparing(a -> a.key().id());

    public PersistenceLinksConnectionFetcher(RxJsonPersistence persistence, String nameSpace, String sourceEntityName, JsonNavigationPath path, String targetEntityName) {
        this.persistence = Objects.requireNonNull(persistence);
        this.nameSpace = Objects.requireNonNull(nameSpace);
        this.sourceEntityName = Objects.requireNonNull(sourceEntityName);
        this.relationPath = Objects.requireNonNull(path);
        this.targetEntityName = Objects.requireNonNull(targetEntityName);
    }

    /**
     * Extracts the id from the source object in the environment.
     */
    private static String getIdFromSource(DataFetchingEnvironment environment) {
        Map<String, Object> source = environment.getSource();
        DocumentKey key = (DocumentKey) source.get("__graphql_internal_document_key");
        return key.id();
    }

    private List<GraphQLNamedOutputType> getConcreteTypes(GraphQLSchema graphQLSchema, String typeName) {
        GraphQLType actualType = graphQLSchema.getType(typeName);
        if (actualType instanceof GraphQLUnionType) {
            return ((GraphQLUnionType) actualType).getTypes();
        } else if (actualType instanceof GraphQLInterfaceType) {
            List<GraphQLNamedOutputType> types = new ArrayList<>();
            for (GraphQLType concreteType : graphQLSchema.getAllTypesAsList()) {
                if (concreteType instanceof GraphQLOutputType &&
                        graphQLSchema.isPossibleType((GraphQLInterfaceType) actualType, (GraphQLObjectType) concreteType)) {
                    types.add((GraphQLObjectType) concreteType);
                }
            }
            return types;
        } else if (actualType instanceof GraphQLNamedOutputType) {
            return List.of((GraphQLNamedOutputType) actualType);
        } else {
            throw new ClassCastException(
                    String.format("the type type %sd (%s) should be a GraphQLOutputType", typeName, actualType)
            );
        }
    }

    @Override
    Connection<Map<String, Object>> getConnection(DataFetchingEnvironment environment, ConnectionParameters parameters) {
        try (Transaction tx = persistence.createTransaction(true)) {

            String sourceId = getIdFromSource(environment);

            // In cases of union type, we need to make several calls.
            List<GraphQLNamedOutputType> concreteTypes = getConcreteTypes(environment.getGraphQLSchema(), targetEntityName);
            Flowable<JsonDocument> documents = Flowable.empty();
            for (GraphQLNamedOutputType concreteType : concreteTypes) {
                Flowable<JsonDocument> concreteDocuments = persistence.readTargetDocuments(tx, parameters.getSnapshot(),
                        nameSpace, sourceEntityName, sourceId, relationPath, concreteType.getName(), parameters.getRange());
                documents = Flowable.merge(documents, concreteDocuments);
            }
            // Limit the flow.
            if (concreteTypes.size() > 1) {
                if (parameters.getRange().isLimited()) {
                    documents = parameters.getRange().isBackward()
                            ? documents.takeLast(parameters.getRange().getLimit())
                            : documents.limit(parameters.getRange().getLimit());
                }
                documents = parameters.getRange().isBackward()
                        ? documents.sorted(BY_ID.reversed())
                        : documents.sorted(BY_ID);
            }
            List<Edge<Map<String, Object>>> edges = documents.map(document -> toEdge(document)).toList()
                    .blockingGet();

            if (edges.isEmpty()) {
                PageInfo pageInfo = new DefaultPageInfo(null, null, false,
                        false);
                return new DefaultConnection<>(Collections.emptyList(), pageInfo);
            }

            Edge<Map<String, Object>> firstEdge = edges.get(0);
            Edge<Map<String, Object>> lastEdge = edges.get(edges.size() - 1);

            boolean hasPrevious = true;
            if (environment.getSelectionSet().contains("pageInfo/hasPreviousPage")) {
                hasPrevious = persistence.readTargetDocuments(tx, parameters.getSnapshot(), nameSpace, sourceEntityName,
                        sourceId, relationPath, targetEntityName, Range.lastBefore(1, firstEdge.getCursor().getValue())
                ).isEmpty().map(wasEmpty -> !wasEmpty).blockingGet();
            }

            boolean hasNext = true;
            if (environment.getSelectionSet().contains("pageInfo/hasNextPage")) {
                hasNext = persistence.readTargetDocuments(tx, parameters.getSnapshot(), nameSpace, sourceEntityName,
                        sourceId, relationPath, targetEntityName, Range.firstAfter(1, lastEdge.getCursor().getValue())
                ).isEmpty().map(wasEmpty -> !wasEmpty).blockingGet();
            }

            PageInfo pageInfo = new DefaultPageInfo(
                    firstEdge.getCursor(),
                    lastEdge.getCursor(),
                    hasPrevious,
                    hasNext
            );

            return new DefaultConnection<>(
                    edges,
                    pageInfo
            );

        }
    }
}
