package no.ssb.lds.graphql.schemas;

import graphql.schema.GraphQLCodeRegistry;
import graphql.schema.GraphQLTypeVisitorStub;
import no.ssb.lds.api.persistence.reactivex.RxJsonPersistence;

import java.util.Objects;

/**
 * GraphQL visitor that setup the code registry with the appropriate fetchers.
 */
public class GraphQLLinkFetcherVisitor extends GraphQLTypeVisitorStub {

    private final RxJsonPersistence persistence;
    private final GraphQLCodeRegistry registry;

    public GraphQLLinkFetcherVisitor(RxJsonPersistence persistence, GraphQLCodeRegistry registry) {
        this.persistence = Objects.requireNonNull(persistence);
        this.registry = Objects.requireNonNull(registry);
    }


}
