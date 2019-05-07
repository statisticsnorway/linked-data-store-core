package no.ssb.lds.graphql.schemas;

import graphql.schema.GraphQLDirective;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLSchema;
import graphql.schema.GraphQLType;
import graphql.schema.GraphQLTypeCollectingVisitor;
import graphql.schema.GraphQLTypeResolvingVisitor;
import graphql.schema.TypeTraverser;
import no.ssb.lds.api.persistence.reactivex.RxJsonPersistence;
import no.ssb.lds.api.search.SearchIndex;
import no.ssb.lds.api.specification.Specification;
import no.ssb.lds.core.specification.JsonSchemaBasedSpecification;
import no.ssb.lds.graphql.directives.DomainDirective;
import no.ssb.lds.graphql.directives.LinkDirective;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class GraphQLSchemaBuilder {

    private static final Logger log = LoggerFactory.getLogger(OldGraphqlSchemaBuilder.class);
    private static final TypeTraverser TRAVERSER = new TypeTraverser();

    private static final String QUERY_NAME = "Query";

    private final Specification specification;
    private final RxJsonPersistence persistence;
    private final SearchIndex searchIndex;
    private final String namespace;
    private final GraphQLObjectType.Builder query = GraphQLObjectType.newObject().name(QUERY_NAME);

    public GraphQLSchemaBuilder(String namespace, Specification specification, RxJsonPersistence persistence,
                                SearchIndex searchIndex) {
        this.specification = specification;
        this.persistence = persistence;
        this.searchIndex = searchIndex;
        this.namespace = namespace;
    }

    /**
     * Use this to print a graphql schema out of a directory of json files.
     */
    public static void main(String... argv) {
        JsonSchemaBasedSpecification spec = JsonSchemaBasedSpecification.create(argv[0]);
        GraphQLSchemaBuilder schemaBuilder = new GraphQLSchemaBuilder("namespace", spec, new EmptyPersistence(), null);
        GraphQLSchema graphQL = schemaBuilder.getGraphQL();
        System.out.println(graphQL);
    }

    private static String printSchema(GraphQLType type) {
        return "";
    }

    private static String printSchema(Collection<GraphQLType> types) {
        StringBuilder builder = new StringBuilder();
        for (GraphQLType type : types) {
            builder.append(printSchema(type));
        }
        return builder.toString();
    }

    public GraphQLSchema getGraphQL() {

        // Convert the specifications to GraphQL types:
        log.info("Converting specification to graphql");
        SpecificationTraverser specificationTraverser = new SpecificationTraverser(specification);
        Collection<GraphQLType> graphQLTypes = specificationTraverser.getGraphQLTypes();

        if (log.isDebugEnabled()) {
            log.debug("Converted specification:\n{}", printSchema(graphQLTypes));
        } else {
            log.info("Converted {} specifications to {} GraphQL types", specification.getManagedDomains().size(),
                    graphQLTypes.size());
        }

        // Collect all the types in a type map.
        log.info("Collecting types into type map");
        GraphQLTypeCollectingVisitor graphQLTypeCollectingVisitor = new GraphQLTypeCollectingVisitor();
        TRAVERSER.depthFirst(graphQLTypeCollectingVisitor, graphQLTypes);
        Map<String, GraphQLType> typeMap = graphQLTypeCollectingVisitor.getResult();

        // Fake reverseName until the annotation is finalized.
        TRAVERSER.depthFirst(new FakeAnnotationVisitor(typeMap), typeMap.values());

        // Add the reverse links.
        log.info("Computing reverse links");
        TRAVERSER.depthFirst(new GraphQLReverseLinkVisitor(typeMap), typeMap.values());

        // Create the query fields.
        log.info("Creating root \"Query\" fields");
        GraphQLQueryBuildingVisitor graphQLQueryVisitor = new GraphQLQueryBuildingVisitor(query);
        TRAVERSER.depthFirst(graphQLQueryVisitor, typeMap.values());
        if (log.isDebugEnabled()) {
            log.debug("Query:\n{}", printSchema(graphQLQueryVisitor.getQuery()));
        }

        // Add the search fields.
        log.info("Creating root \"Query\" search field and search types");
        GraphQLQuerySearchVisitor graphQLQuerySearchVisitor = new GraphQLQuerySearchVisitor(typeMap, query);
        TRAVERSER.depthFirst(graphQLQuerySearchVisitor, typeMap.values());
        if (log.isDebugEnabled()) {
            log.debug("Query:\n{}", printSchema(graphQLQueryVisitor.getQuery()));
        }

        // Transform with pagination
        log.info("Transforming paginated links to relay connections");
        GraphQLPaginationVisitor graphQLPaginationVisitor = new GraphQLPaginationVisitor(typeMap);
        TRAVERSER.depthFirst(graphQLPaginationVisitor, typeMap.values());

        log.info("Resolving type references");
        GraphQLTypeResolvingVisitor typeResolvingVisitor = new GraphQLTypeResolvingVisitor(typeMap);
        TRAVERSER.depthFirst(typeResolvingVisitor, typeMap.values());

        if (log.isDebugEnabled()) {
            log.debug("Final schema:\n{}", printSchema(typeMap.values()));
        }

        Set<GraphQLDirective> directives = Set.of(
                DomainDirective.INSTANCE,
                LinkDirective.INSTANCE
        );

        return GraphQLSchema.newSchema()
                .query(query)
                .additionalTypes(new HashSet<>(typeMap.values()))
                .additionalDirectives(directives)
                .build();
    }


}
