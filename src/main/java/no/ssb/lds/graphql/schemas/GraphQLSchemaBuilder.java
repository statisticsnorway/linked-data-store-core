package no.ssb.lds.graphql.schemas;

import graphql.scalars.ExtendedScalars;
import graphql.schema.GraphQLDirective;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLSchema;
import graphql.schema.GraphQLType;
import graphql.schema.GraphQLTypeCollectingVisitor;
import graphql.schema.GraphQLTypeResolvingVisitor;
import graphql.schema.TypeTraverser;
import graphql.schema.idl.EchoingWiringFactory;
import graphql.schema.idl.RuntimeWiring;
import graphql.schema.idl.SchemaGenerator;
import graphql.schema.idl.SchemaParser;
import graphql.schema.idl.SchemaPrinter;
import graphql.schema.idl.TypeDefinitionRegistry;
import no.ssb.lds.api.persistence.reactivex.RxJsonPersistence;
import no.ssb.lds.api.search.SearchIndex;
import no.ssb.lds.core.specification.JsonSchemaBasedSpecification;
import no.ssb.lds.graphql.directives.DomainDirective;
import no.ssb.lds.graphql.directives.LinkDirective;
import no.ssb.lds.graphql.directives.ReverseLinkDirective;
import no.ssb.lds.graphql.schemas.visitors.AddConnectionVisitor;
import no.ssb.lds.graphql.schemas.visitors.AddSearchTypesVisitor;
import no.ssb.lds.graphql.schemas.visitors.AutomaticReverseLink;
import no.ssb.lds.graphql.schemas.visitors.GraphQLTypeReferencerVisitor;
import no.ssb.lds.graphql.schemas.visitors.QueryBuildingVisitor;
import no.ssb.lds.graphql.schemas.visitors.RegistrySetupVisitor;
import no.ssb.lds.graphql.schemas.visitors.ReverseLinkBuildingVisitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class GraphQLSchemaBuilder {

    private static final Logger log = LoggerFactory.getLogger(GraphQLSchemaBuilder.class);
    private static final TypeTraverser TRAVERSER = new TypeTraverser();
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

    /**
     * Use this to print a graphql schema out of a directory of json files.
     */
    public static void main(String... argv) {

        Boolean graphQl = false;
        Boolean jsonSchema = false;
        Deque<String> arguments = new ArrayDeque<>(Arrays.asList(argv));
        String current = arguments.peek();
        while (!arguments.isEmpty()) {
            if ("--graphql".equals(current)) {
                graphQl = true;
            }
            if ("--json-schema".equals(current)) {
                jsonSchema = true;
            }
            current = arguments.pop();
        }

        GraphQLSchemaBuilder schemaBuilder = new GraphQLSchemaBuilder("namespace", new EmptyPersistence(), null);

        GraphQLSchema schema;
        if (graphQl && !jsonSchema) {
            log.info("Parsing graphql schema {}", current);
            TypeDefinitionRegistry definitionRegistry = new SchemaParser().parse(new File(current));
            schema = schemaBuilder.parseSchema(definitionRegistry);
        } else if (jsonSchema && !graphQl) {
            log.info("Parsing json-schemas in {}", current);
            JsonSchemaBasedSpecification specification = JsonSchemaBasedSpecification.create(current);
            SpecificationConverter test = new SpecificationConverter();
            //schema = schemaBuilder.parseSpecification(specification);
            schema = schemaBuilder.parseSchema(test.convert(specification));
        } else {
            log.error("Usage: --graphql file | --json-schema folder");
            return;
        }
        GraphQLSchema graphQL = schemaBuilder.getGraphQL(schema);
    }

    private static String printSchema(GraphQLType type) {
        return PRINTER.print(type);
    }

    private static String printSchema(Collection<GraphQLType> types) {
        StringBuilder builder = new StringBuilder();
        for (GraphQLType type : types) {
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
                SchemaGenerator.Options.defaultOptions().enforceSchemaDirectives(true),
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
        Map<String, GraphQLType> typeMap = graphQLTypeCollectingVisitor.getResult();

        // Reference and Resolve
        TRAVERSER.depthFirst(new GraphQLTypeReferencerVisitor(typeMap), typeMap.values());
        TRAVERSER.depthFirst(new GraphQLTypeResolvingVisitor(typeMap), typeMap.values());

        // Compute reverse links
        TRAVERSER.depthFirst(new AutomaticReverseLink(typeMap), typeMap.values());

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
            GraphQLType queryWithSearch = addSearchTypesVisitor.getQuery();
            if (log.isTraceEnabled()) {
                log.trace("Query:\n{}", printSchema(queryWithSearch));
            }
        }

        // Done with the query at this point so add it to the type map for the next passes.
        typeMap.put("Query", query.build());

        log.info("Replacing all type with references");
        TRAVERSER.depthFirst(new GraphQLTypeReferencerVisitor(typeMap), typeMap.values());

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