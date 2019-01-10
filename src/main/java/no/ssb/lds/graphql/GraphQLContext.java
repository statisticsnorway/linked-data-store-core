package no.ssb.lds.graphql;

import io.undertow.server.HttpServerExchange;

import java.time.ZonedDateTime;

/**
 * The context object passed around in the lds graphql executions.'
 */
public interface GraphQLContext {

    // TODO: Find a better name. This clashes with graphql-java objects.
    
    /**
     * Returns the undertow server exchange that triggered the execution.
     */
    HttpServerExchange getExchange();

    /**
     * Returns the snapshot used to retrieve data from persistence
     */
    ZonedDateTime getSnapshot();
}
