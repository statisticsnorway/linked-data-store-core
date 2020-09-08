package no.ssb.lds.graphql.schemas;

import graphql.scalars.ExtendedScalars;
import graphql.schema.GraphQLDirective;
import graphql.schema.GraphQLNamedType;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLSchema;
import graphql.schema.GraphQLType;
import graphql.schema.GraphQLTypeCollectingVisitor;
import graphql.schema.GraphQLTypeResolvingVisitor;
import graphql.schema.SchemaTraverser;
import graphql.schema.idl.EchoingWiringFactory;
import graphql.schema.idl.RuntimeWiring;
import graphql.schema.idl.SchemaGenerator;
import graphql.schema.idl.SchemaPrinter;
import graphql.schema.idl.TypeDefinitionRegistry;
import no.ssb.lds.api.persistence.reactivex.RxJsonPersistence;
import no.ssb.lds.api.search.SearchIndex;
import no.ssb.lds.graphql.directives.DomainDirective;
import no.ssb.lds.graphql.directives.LinkDirective;
import no.ssb.lds.graphql.directives.ReverseLinkDirective;
import no.ssb.lds.graphql.schemas.visitors.AddConnectionVisitor;
import no.ssb.lds.graphql.schemas.visitors.AddSearchTypesVisitor;
import no.ssb.lds.graphql.schemas.visitors.QueryBuildingVisitor;
import no.ssb.lds.graphql.schemas.visitors.RegistrySetupVisitor;
import no.ssb.lds.graphql.schemas.visitors.ReverseLinkBuildingVisitor;
import no.ssb.lds.graphql.schemas.visitors.ReverseLinkNameVisitor;
import no.ssb.lds.graphql.schemas.visitors.TypeReferencerVisitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class GraphQLSchemaBuilder {

    private static final Logger log = LoggerFactory.getLogger(GraphQLSchemaBuilder.class);
    private static final SchemaTraverser TRAVERSER = new SchemaTraverser();
    private static final SchemaPrinter PRINTER = new SchemaPrinter();
    private static final String QUERY_NAME = "Query";
    private final RxJsonPersistence persistence;
    private final SearchIndex searchIndex;
    private final String namespace;
    private final GraphQLObjectType.Builder query = GraphQLObjectType.newObject().name(QUERY_NAME);

    public GraphQLSchemaBuilder(String namespace, RxJsonPersistence persistence,
                                SearchIndex searchIndex) {
        this.persistence = persistence;
        this.searchIndex = searchIndex;
        this.namespace = namespace;
    }

    private static String printSchema(GraphQLNamedType type) {
        return PRINTER.print(type);
    }

    private static String printSchema(Collection<GraphQLNamedType> types) {
        StringBuilder builder = new StringBuilder();
        for (GraphQLNamedType type : types) {
            builder.append(printSchema(type));
        }
        return builder.toString();
    }

    public static GraphQLSchema parseSchema(TypeDefinitionRegistry registry) {

        RuntimeWiring.Builder runtime = RuntimeWiring.newRuntimeWiring()
                .scalar(ExtendedScalars.DateTime)
                .scalar(ExtendedScalars.Date)
                .scalar(ExtendedScalars.Time);

        runtime.wiringFactory(new EchoingWiringFactory());

        return new SchemaGenerator().makeExecutableSchema(
                SchemaGenerator.Options.defaultOptions(),
                registry,
                runtime.build()
        );
    }

    public GraphQLSchema getGraphQL(GraphQLSchema schema) {

        Map<String, GraphQLType> graphQLTypes = new TreeMap<>(schema.getTypeMap());
        graphQLTypes.remove("Query");
        graphQLTypes.keySet().removeIf(key -> key.startsWith("__"));

        // Collect all the types in a type map.
        log.info("Collecting types into type map");
        GraphQLTypeCollectingVisitor graphQLTypeCollectingVisitor = new GraphQLTypeCollectingVisitor();
        TRAVERSER.depthFirst(graphQLTypeCollectingVisitor, graphQLTypes.values());
        Map<String, GraphQLNamedType> typeMap = graphQLTypeCollectingVisitor.getResult();

        // Reference and Resolve
        TRAVERSER.depthFirst(new TypeReferencerVisitor(typeMap), typeMap.values());
        TRAVERSER.depthFirst(new GraphQLTypeResolvingVisitor(typeMap), typeMap.values());

        // Compute reverse links
        TRAVERSER.depthFirst(new ReverseLinkNameVisitor(typeMap), typeMap.values());

        // Add the reverse links.
        TRAVERSER.depthFirst(new ReverseLinkBuildingVisitor(typeMap), typeMap.values());
        if (log.isTraceEnabled()) {
            log.trace("Computing reverse links:\n{}", printSchema(typeMap.values()));
        } else {
            log.info("Computing reverse links");
        }

        // Create the query fields.
        log.info("Creating root \"Query\" fields");
        QueryBuildingVisitor graphQLQueryVisitor = new QueryBuildingVisitor(query);
        TRAVERSER.depthFirst(graphQLQueryVisitor, typeMap.values());
        if (log.isTraceEnabled()) {
            log.trace("Query:\n{}", printSchema(graphQLQueryVisitor.getQuery()));
        }

        // Add the search fields.
        if (searchIndex != null) {
            log.info("Creating root \"Query\" search field and search types");
            AddSearchTypesVisitor addSearchTypesVisitor = new AddSearchTypesVisitor(typeMap, query);
            TRAVERSER.depthFirst(addSearchTypesVisitor, typeMap.values());
            GraphQLNamedType queryWithSearch = addSearchTypesVisitor.getQuery();
            if (log.isTraceEnabled()) {
                log.trace("Query:\n{}", printSchema(queryWithSearch));
            }
        }

        // Done with the query at this point so add it to the type map for the next passes.
        typeMap.put("Query", query.build());

        log.info("Replacing all type with references");
        TRAVERSER.depthFirst(new TypeReferencerVisitor(typeMap), typeMap.values());

        // Transform with pagination
        log.info("Transforming paginated links to relay connections");
        AddConnectionVisitor addConnectionVisitor = new AddConnectionVisitor(typeMap);
        TRAVERSER.depthFirst(addConnectionVisitor, typeMap.values());

        if (log.isDebugEnabled()) {
            log.debug("Final schema:\n{}", printSchema(typeMap.values()));
        }

        Set<GraphQLDirective> directives = Set.of(
                DomainDirective.INSTANCE,
                LinkDirective.INSTANCE,
                ReverseLinkDirective.INSTANCE
        );

        // Resolve references before using RegistrySetupVisitor
        log.info("Resolving type references");
        GraphQLTypeResolvingVisitor typeResolvingVisitor = new GraphQLTypeResolvingVisitor(typeMap);
        TRAVERSER.depthFirst(typeResolvingVisitor, typeMap.values());

        RegistrySetupVisitor fetcherSetupVisitor = new RegistrySetupVisitor(persistence, namespace, searchIndex);
        TRAVERSER.depthFirst(fetcherSetupVisitor, typeMap.values());

        GraphQLType queryType = typeMap.remove("Query");

        return GraphQLSchema.newSchema()
                .query((GraphQLObjectType) queryType)
                .additionalTypes(new HashSet<>(typeMap.values()))
                .additionalDirectives(directives)
                .codeRegistry(fetcherSetupVisitor.getRegistry())
                .build();
    }
}
