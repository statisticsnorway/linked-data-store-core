package no.ssb.lds.core;

import com.netflix.hystrix.HystrixThreadPoolProperties;
import graphql.GraphQL;
import graphql.schema.GraphQLSchema;
import graphql.schema.idl.SchemaParser;
import graphql.schema.idl.TypeDefinitionRegistry;
import io.undertow.Handlers;
import io.undertow.Undertow;
import io.undertow.server.HttpHandler;
import io.undertow.server.handlers.PathHandler;
import io.undertow.server.handlers.ResponseCodeHandler;
import io.undertow.server.handlers.resource.ClassPathResourceManager;
import io.undertow.util.StatusCodes;
import no.ssb.concurrent.futureselector.SelectableThreadPoolExectutor;
import no.ssb.config.DynamicConfiguration;
import no.ssb.lds.api.persistence.reactivex.RxJsonPersistence;
import no.ssb.lds.api.search.SearchIndex;
import no.ssb.lds.api.specification.Specification;
import no.ssb.lds.core.controller.CORSHandler;
import no.ssb.lds.core.controller.HealthCheckHandler;
import no.ssb.lds.core.controller.NamespaceController;
import no.ssb.lds.core.persistence.PersistenceConfigurator;
import no.ssb.lds.core.saga.SagaExecutionCoordinator;
import no.ssb.lds.core.saga.SagaRecoveryTrigger;
import no.ssb.lds.core.saga.SagaRepository;
import no.ssb.lds.core.saga.SagasObserver;
import no.ssb.lds.core.schema.JsonSchema;
import no.ssb.lds.core.schema.JsonSchema04Builder;
import no.ssb.lds.core.search.SearchIndexConfigurator;
import no.ssb.lds.core.specification.JsonSchemaBasedSpecification;
import no.ssb.lds.core.specification.SpecificationJsonSchemaBuilder;
import no.ssb.lds.core.txlog.TxlogRawdataPool;
import no.ssb.lds.core.utils.LDSProviderConfigurator;
import no.ssb.lds.graphql.GraphqlHttpHandler;
import no.ssb.lds.graphql.directives.DomainDirective;
import no.ssb.lds.graphql.directives.LinkDirective;
import no.ssb.lds.graphql.directives.ReverseLinkDirective;
import no.ssb.lds.graphql.jsonSchema.GraphQLToJsonConverter;
import no.ssb.lds.graphql.schemas.GraphQLSchemaBuilder;
import no.ssb.lds.graphqlneo4j.GraphQLNeo4jHttpHandler;
import no.ssb.lds.graphqlneo4j.GraphQLNeo4jTBVSchemas;
import no.ssb.rawdata.api.RawdataClient;
import no.ssb.rawdata.api.RawdataClientInitializer;
import no.ssb.sagalog.SagaLogInitializer;
import no.ssb.sagalog.SagaLogPool;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Optional.ofNullable;

public class UndertowApplication {

    private static final Logger LOG = LoggerFactory.getLogger(UndertowApplication.class);
    private final RxJsonPersistence persistence;
    private final Specification specification;
    private final Undertow server;
    private final String host;
    private final int port;
    private final SagaExecutionCoordinator sec;
    private final SagaRepository sagaRepository;
    private final SagasObserver sagasObserver;
    private final SagaLogPool sagaLogPool;
    private final SelectableThreadPoolExectutor sagaThreadPool;
    private final SagaRecoveryTrigger sagaRecoveryTrigger;
    private final TxlogRawdataPool txlogRawdataPool;

    UndertowApplication(Specification specification, RxJsonPersistence persistence, SagaExecutionCoordinator sec,
                        SagaRepository sagaRepository, SagasObserver sagasObserver, SagaRecoveryTrigger sagaRecoveryTrigger, String host, int port,
                        SagaLogPool sagaLogPool, SelectableThreadPoolExectutor sagaThreadPool,
                        NamespaceController namespaceController, SearchIndex searchIndex,
                        DynamicConfiguration configuration, TxlogRawdataPool txlogRawdataPool) {
        this.specification = specification;
        this.sagaRecoveryTrigger = sagaRecoveryTrigger;
        this.host = host;
        this.port = port;
        this.persistence = persistence;
        this.sec = sec;
        this.sagaRepository = sagaRepository;
        this.sagasObserver = sagasObserver;
        this.sagaLogPool = sagaLogPool;
        this.sagaThreadPool = sagaThreadPool;
        this.txlogRawdataPool = txlogRawdataPool;

        LOG.info("Initializing Http handlers ...");

        String namespace = configuration.evaluateToString("namespace.default");
        boolean enableRequestDump = configuration.evaluateToBoolean("http.request.dump");
        boolean graphqlEnabled = configuration.evaluateToBoolean("graphql.enabled");
        String pathPrefix = configuration.evaluateToString("http.prefix");

        Optional<String> graphQLSchemaPath = ofNullable(configuration.evaluateToString("graphql.schema"))
                .map(path -> path.isEmpty() ? null : path);

        PathHandler pathHandler = Handlers.path();
        if (graphqlEnabled && graphQLSchemaPath.isPresent()) {

            LOG.info("Initializing GraphQL Web API ...");

            GraphQLSchemaBuilder schemaBuilder = new GraphQLSchemaBuilder(namespace, persistence, searchIndex);
            File graphQLFile = new File(graphQLSchemaPath.get());

            TypeDefinitionRegistry definitionRegistry = parseSchemaFile(graphQLFile);

            final String providerId = configuration.evaluateToString("persistence.provider");

            HttpHandler graphQLHttpHandler;

            if ("neo4j".equals(providerId)) {

                LOG.info("Initializing GraphQL Neo4j integration ...");

                GraphQLSchema schema = GraphQLNeo4jTBVSchemas.schemaOf(GraphQLNeo4jTBVSchemas.transformRegistry(definitionRegistry));
                GraphQL graphQL = GraphQL.newGraphQL(schema).build();
                graphQLHttpHandler = new GraphQLNeo4jHttpHandler(graphQL);
            } else {

                LOG.info("Initializing GraphQL integration ...");

                GraphQLSchema schema = schemaBuilder.getGraphQL(GraphQLSchemaBuilder.parseSchema(definitionRegistry));
                GraphQL graphQL = GraphQL.newGraphQL(schema).build();
                graphQLHttpHandler = new GraphqlHttpHandler(graphQL);
            }

            pathHandler.addExactPath("/graphql", graphQLHttpHandler);

            pathHandler.addExactPath("/graphiql", Handlers.resource(new ClassPathResourceManager(
                    Thread.currentThread().getContextClassLoader(), "no/ssb/lds/graphql/graphiql"
            )).setDirectoryListingEnabled(false).addWelcomeFiles("graphiql.html"));
        }

        LOG.info("Initializing health handlers ...");

        HealthCheckHandler healthHandler = new HealthCheckHandler(persistence);
        pathHandler.addExactPath(HealthCheckHandler.HEALTH_ALIVE_PATH, healthHandler);
        ResponseCodeHandler aliveHandler = new ResponseCodeHandler(StatusCodes.OK);
        pathHandler.addExactPath(HealthCheckHandler.HEALTH_READY_PATH, aliveHandler);
        pathHandler.addExactPath(HealthCheckHandler.PING_PATH, aliveHandler);
        pathHandler.addPrefixPath("/", namespaceController);

        HttpHandler httpHandler;
        if (enableRequestDump) {
            LOG.info("Initializing request-dump ...");
            httpHandler = Handlers.requestDump(pathHandler);
        } else {
            httpHandler = pathHandler;
        }

        if (pathPrefix != null && !pathPrefix.isEmpty()) {
            LOG.info("Using http prefix: {}", pathPrefix);
            httpHandler = Handlers.path(ResponseCodeHandler.HANDLE_404).addPrefixPath(pathPrefix, httpHandler);
        }

        LOG.info("Initializing CORS-handler ...");

        List<Pattern> corsAllowOrigin = Stream.of(configuration.evaluateToString("http.cors.allow.origin")
                .split(",")).map(Pattern::compile).collect(Collectors.toUnmodifiableList());
        Set<String> corsAllowMethods = Set.of(configuration.evaluateToString("http.cors.allow.methods")
                .split(","));
        Set<String> corsAllowHeaders = Set.of(configuration.evaluateToString("http.cors.allow.header")
                .split(","));
        boolean corsAllowCredentials = configuration.evaluateToBoolean("http.cors.allow.credentials");
        int corsMaxAge = configuration.evaluateToInt("http.cors.allow.max-age");

        CORSHandler corsHandler = new CORSHandler(httpHandler, httpHandler, corsAllowOrigin,
                corsAllowCredentials, StatusCodes.NO_CONTENT, corsMaxAge, corsAllowMethods, corsAllowHeaders
        );

        LOG.info("Initializing Undertow ...");

        this.server = Undertow.builder()
                .addHttpListener(port, host)
                .setHandler(corsHandler)
                .build();
    }

    private static TypeDefinitionRegistry parseSchemaFile(File graphQLFile) {
        TypeDefinitionRegistry definitionRegistry;
        URL systemResource = ClassLoader.getSystemResource(graphQLFile.getPath());

        if (ofNullable(systemResource).isPresent()) {
            definitionRegistry = new SchemaParser().parse(new File(systemResource.getPath()));
        } else {
            definitionRegistry = new SchemaParser().parse(new File(graphQLFile.getPath()));
        }
        return definitionRegistry;
    }

    public static String getDefaultConfigurationResourcePath() {
        return "application-defaults.properties";
    }

    public static UndertowApplication initializeUndertowApplication(DynamicConfiguration configuration) {
        int port = configuration.evaluateToInt("http.port");
        return initializeUndertowApplication(configuration, port);
    }

    public static UndertowApplication initializeUndertowApplication(DynamicConfiguration configuration, int port) {
        LOG.info("Initializing Linked Data Store (LDS) server ...");

        LOG.info("Initializing specification ...");

        JsonSchemaBasedSpecification specification;

        Optional<String> graphQLSchemaPath = ofNullable(configuration.evaluateToString("graphql.schema"))
                .map(path -> path.isEmpty() ? null : path);

        if (graphQLSchemaPath.isPresent()) {
            File graphQLFile = new File(graphQLSchemaPath.get());

            LOG.info("Using GraphQL file: {}", graphQLFile.toString());

            TypeDefinitionRegistry definitionRegistry = parseSchemaFile(graphQLFile);

            GraphQLSchema schema;
            final String providerId = configuration.evaluateToString("persistence.provider");

            if ("neo4j".equals(providerId)) {

                LOG.info("Transforming GraphQL schema to conform with GRANDstack compatible Neo4j modelling for Specification purposes");
                schema = GraphQLNeo4jTBVSchemas.schemaOf(GraphQLNeo4jTBVSchemas.transformRegistry(definitionRegistry)).transform(builder -> {
                    // TODO figure out what is missing that GraphQLSchemaBuilder.parseSchema(definitionRegistry); does
                    builder.additionalDirectives(Set.of(
                            DomainDirective.INSTANCE,
                            LinkDirective.INSTANCE,
                            ReverseLinkDirective.INSTANCE
                    ));
                });

            } else {

                LOG.info("Using GraphQL schema as defined directly in SDL file for Specification purposes");
                schema = GraphQLSchemaBuilder.parseSchema(definitionRegistry);
            }

            GraphQLToJsonConverter graphQLToJsonConverter = new GraphQLToJsonConverter(schema);
            LinkedHashMap<String, JSONObject> jsonMap = graphQLToJsonConverter.createSpecification(schema);

            specification = createJsonSpecification(definitionRegistry, jsonMap);

        } else {
            String schemaConfigStr = configuration.evaluateToString("specification.schema");
            String[] specificationSchema = ("".equals(schemaConfigStr) ? new String[0] : schemaConfigStr.split(","));
            LOG.info("Creating specification using json-schema: {}", schemaConfigStr);
            specification = JsonSchemaBasedSpecification.create(null, specificationSchema);
        }

        LOG.info("Initializing primary persistence ...");

        RxJsonPersistence persistence = PersistenceConfigurator.configurePersistence(configuration, specification);

        LOG.info("Initializing saga-log pool ...");

        SagaLogPool sagaLogPool = configureSagaLogProvider(configuration);

        SagaRepository.Builder sagaRepositoryBuilder = new SagaRepository.Builder()
                .specification(specification)
                .persistence(persistence);

        LOG.info("Initializing search-index ...");

        SearchIndex searchIndex = SearchIndexConfigurator.configureSearchIndex(configuration);
        if (searchIndex != null) {
            sagaRepositoryBuilder.indexer(searchIndex);
        }

        LOG.info("Initializing transaction-log ...");

        RawdataClient txLogClient = configureTxLogRawdataClient(configuration);
        boolean splitSources = configuration.evaluateToBoolean("txlog.split.sources");
        String defaultSource = ofNullable(configuration.evaluateToString("txlog.default-source")).filter(s -> !s.isBlank()).orElse("default");
        String txLogTopicPrefix = ofNullable(configuration.evaluateToString("txlog.rawdata.topic-prefix")).map(String::trim).orElse("");
        TxlogRawdataPool txlogRawdataPool = new TxlogRawdataPool(txLogClient, splitSources, defaultSource, txLogTopicPrefix);
        sagaRepositoryBuilder.txLogRawdataPool(txlogRawdataPool);

        LOG.info("Initializing saga repository ...");

        SagaRepository sagaRepository = sagaRepositoryBuilder.build();

        LOG.info("Initializing saga observer ...");

        final SagasObserver sagasObserver = new SagasObserver(sagaRepository).start();

        LOG.info("Initializing saga thread-pool ...");

        final AtomicLong nextWorkerId = new AtomicLong(1);
        int sagaThreadPoolQueueCapacity = configuration.evaluateToInt("saga.threadpool.queue.capacity");
        int sagaThreadPoolCoreSize = configuration.evaluateToInt("saga.threadpool.core");
        if (sagaThreadPoolQueueCapacity >= sagaThreadPoolCoreSize) {
            LOG.warn("Configuration: saga.threadpool.core ({}) must be greater than saga.threadpool.queue.capacity ({}) in order to avoid potential deadlocks.",
                    sagaThreadPoolCoreSize, sagaThreadPoolQueueCapacity);
        }
        SelectableThreadPoolExectutor sagaThreadPool = new SelectableThreadPoolExectutor(
                sagaThreadPoolCoreSize, configuration.evaluateToInt("saga.threadpool.max"),
                configuration.evaluateToInt("saga.threadpool.keepalive.seconds"), TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(sagaThreadPoolQueueCapacity),
                runnable -> {
                    Thread thread = new Thread(runnable);
                    thread.setName("sec-" + nextWorkerId.getAndIncrement());
                    thread.setUncaughtExceptionHandler((t, e) -> {
                        System.err.println("Uncaught exception in thread " + thread.getName());
                        e.printStackTrace();
                    });
                    return thread;
                },
                new ThreadPoolExecutor.AbortPolicy()
        );
        int numberOfSagaLogs = configuration.evaluateToInt("saga.number-of-logs");

        boolean sagaCommandsEnabled = configuration.evaluateToBoolean("saga.commands.enabled");

        LOG.info("Initializing saga-recovery thread-pool ...");

        AtomicInteger executorThreadId = new AtomicInteger();
        ScheduledExecutorService recoveryThreadPool = Executors.newScheduledThreadPool(10, runnable -> new Thread(runnable, "saga-recovery-" + executorThreadId.incrementAndGet()));

        LOG.info("Initializing saga-execution-coordinator ...");

        SagaExecutionCoordinator sec = new SagaExecutionCoordinator(sagaLogPool, numberOfSagaLogs, sagaRepository, sagasObserver, sagaThreadPool, sagaCommandsEnabled, recoveryThreadPool);

        HystrixThreadPoolProperties.Setter().withMaximumSize(50); // TODO Configure Hystrix properly

        LOG.info("Initializing saga-recovery-trigger ...");

        boolean sagaRecoveryEnabled = configuration.evaluateToBoolean("saga.recovery.enabled");
        SagaRecoveryTrigger sagaRecoveryTrigger = null;
        if (sagaRecoveryEnabled) {
            int intervalMinSec = configuration.evaluateToInt("saga.recovery.interval.seconds.min");
            int intervalMaxSec = configuration.evaluateToInt("saga.recovery.interval.seconds.max");
            sagaRecoveryTrigger = new SagaRecoveryTrigger(sec, intervalMinSec, intervalMaxSec, recoveryThreadPool);
        }

        LOG.info("Initializing namespace-controller ...");

        NamespaceController namespaceController = new NamespaceController(
                configuration.evaluateToString("namespace.default"),
                specification,
                specification,
                persistence,
                sec,
                sagaRepository,
                txlogRawdataPool
        );

        String host = configuration.evaluateToString("http.host");

        return new UndertowApplication(specification, persistence, sec, sagaRepository, sagasObserver, sagaRecoveryTrigger, host, port,
                sagaLogPool, sagaThreadPool, namespaceController,
                searchIndex, configuration, txlogRawdataPool);
    }

    private static JsonSchemaBasedSpecification createJsonSpecification(TypeDefinitionRegistry typeDefinitionRegistry, LinkedHashMap<String, JSONObject> jsonMap) {
        JsonSchemaBasedSpecification jsonSchemaBasedSpecification = null;
        Set<Map.Entry<String, JSONObject>> entries = jsonMap.entrySet();
        Iterator<Map.Entry<String, JSONObject>> iterator = entries.iterator();

        JsonSchema jsonSchema = null;
        while (iterator.hasNext()) {
            Map.Entry item = iterator.next();
            jsonSchema = new JsonSchema04Builder(jsonSchema, item.getKey().toString(), item.getValue().toString()).build();
            jsonSchemaBasedSpecification = SpecificationJsonSchemaBuilder.createBuilder(typeDefinitionRegistry, jsonSchema).build();
        }

        return jsonSchemaBasedSpecification;
    }

    private static SagaLogPool configureSagaLogProvider(DynamicConfiguration configuration) {
        String providerClass = configuration.evaluateToString("sagalog.provider");
        LOG.info("Using saga-log-pool provider-class: {}", providerClass);
        ServiceLoader<SagaLogInitializer> loader = ServiceLoader.load(SagaLogInitializer.class);
        SagaLogInitializer initializer = loader.stream().filter(c -> providerClass.equals(c.type().getName())).findFirst().orElseThrow().get();
        Map<String, String> providerConfig = subMapFromPrefix(configuration.asMap(), "sagalog.config.");

        int retryIntervalSeconds = configuration.evaluateToInt("sagalog.provider.initialization.retry-interval-seconds");
        int maxWaitSeconds = configuration.evaluateToInt("sagalog.provider.initialization.max-wait-seconds");
        long start = System.currentTimeMillis();
        for (int i = 1; ; i++) {
            try {
                LOG.info("SagaLogPool provider initialization attempt # {}", i);
                SagaLogPool sagaLogPool = initializer.initialize(providerConfig);
                LOG.info("SagaLogPool initialized!");
                return sagaLogPool;
            } catch (Exception e) {
                long durationMs = System.currentTimeMillis() - start;
                long remainingMs = TimeUnit.SECONDS.toMillis(maxWaitSeconds) - durationMs;
                if (remainingMs > 0) {
                    try {
                        Thread.sleep(Math.min(retryIntervalSeconds * 1000, remainingMs));
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Interrupted while attempting to sleep, interrupt status preserved.", e);
                    }
                } else {
                    throw e;
                }
            }
        }
    }

    public static RawdataClient configureTxLogRawdataClient(DynamicConfiguration configuration) {
        String provider = configuration.evaluateToString("txlog.rawdata.provider");
        LOG.info("Using transaction-log provider: {}", provider);
        Map<String, String> providerConfig = subMapFromPrefix(configuration.asMap(), "txlog.config.");
        RawdataClientInitializer clientInitializer = LDSProviderConfigurator.configure(providerConfig, provider, RawdataClientInitializer.class);

        int retryIntervalSeconds = configuration.evaluateToInt("txlog.rawdata.provider.initialization.retry-interval-seconds");
        int maxWaitSeconds = configuration.evaluateToInt("txlog.rawdata.provider.initialization.max-wait-seconds");
        long start = System.currentTimeMillis();
        for (int i = 1; ; i++) {
            try {
                LOG.info("Transaction-log provider initialization attempt # {}", i);
                RawdataClient txLogRawdataClient = clientInitializer.initialize(providerConfig);
                LOG.info("Transaction-log initialized");
                return txLogRawdataClient;
            } catch (Exception e) {
                long durationMs = System.currentTimeMillis() - start;
                long remainingMs = TimeUnit.SECONDS.toMillis(maxWaitSeconds) - durationMs;
                if (remainingMs > 0) {
                    try {
                        Thread.sleep(Math.min(retryIntervalSeconds * 1000, remainingMs));
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Interrupted while attempting to sleep, interrupt status preserved.", e);
                    }
                } else {
                    throw e;
                }
            }
        }
    }

    public static Map<String, String> subMapFromPrefix(Map<String, String> configMap, String prefix) {
        NavigableMap<String, String> navConf;
        if (configMap instanceof NavigableMap) {
            navConf = (NavigableMap) configMap;
        } else {
            navConf = new TreeMap<>(configMap);
        }
        return navConf.subMap(prefix, true, prefix + "\uFFFF", false)
                .entrySet().stream().collect(Collectors.toMap(e -> e.getKey().substring(prefix.length()), Map.Entry::getValue));
    }

    static void shutdownAndAwaitTermination(ExecutorService pool) {
        pool.shutdown();
        try {
            if (!pool.awaitTermination(10, TimeUnit.SECONDS)) {
                pool.shutdownNow();
                if (!pool.awaitTermination(10, TimeUnit.SECONDS)) {
                    LOG.error("Pool did not terminate");
                }
            }
        } catch (InterruptedException ie) {
            pool.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    public void enableSagaExecutionAutomaticDeadlockDetectionAndResolution() {
        sec.startThreadpoolWatchdog();
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public void start() {
        LOG.info("Starting Undertow ...");
        server.start();
        if (sagaRecoveryTrigger != null) {
            LOG.info("Starting saga-recovery (instance-local) ...");
            // attempt to recover local saga-logs immediately, then attempt cluster wide recovery regularly
            sec.completeLocalIncompleteSagas(sec.getRecoveryThreadPool()).handle((v, t) -> {
                if (t != null) {
                    LOG.error("Error during initial local saga-logs recovery attempt", t);
                }
                LOG.info("Starting saga-recovery-trigger ...");
                sagaRecoveryTrigger.start();
                return (Void) null;
            });
        }
        LOG.info("Started Linked Data Store server. PID {}", ProcessHandle.current().pid());
        LOG.info("Listening on {}:{}", host, port);
    }

    public void stop() {
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        futures.add(CompletableFuture.runAsync(() -> {
            server.stop();
            LOG.debug("Undertow was shutdown");
        }));
        futures.add(CompletableFuture.runAsync(() -> {
            persistence.close();
            LOG.debug("Persistence provider was shutdown");
        }));
        futures.add(CompletableFuture.runAsync(() -> {
            try {
                txlogRawdataPool.getClient().close();
                LOG.debug("Transaction log (rawdata client) was shutdown");
            } catch (Error | RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }));
        futures.add(CompletableFuture.runAsync(() -> {
            sagasObserver.shutdown();
            LOG.debug("SagaObserver was shutdown");
        }));
        if (sagaRecoveryTrigger != null) {
            futures.add(CompletableFuture.runAsync(sagaRecoveryTrigger::stop));
            LOG.debug("SagaRecoveryTrigger was shutdown");
        }
        futures.add(CompletableFuture.runAsync(() -> {
            sec.shutdown();
            LOG.debug("Saga Execution Coordinator was shutdown");
        }));
        futures.add(CompletableFuture.runAsync(() -> {
            shutdownAndAwaitTermination(sagaThreadPool);
            LOG.debug("Saga thread-pool was shutdown");
        }));
        futures.add(CompletableFuture.runAsync(() -> {
            sagaLogPool.shutdown();
            LOG.debug("SagaLogPool was shutdown");
        }));
        CompletableFuture<Void> all = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
        try {
            all.orTimeout(10, TimeUnit.SECONDS).join();
            LOG.debug("All internal services was shutdown.");
        } catch (CompletionException e) {
            if (e.getCause() != null && e.getCause() instanceof TimeoutException) {
                LOG.warn("Timeout before shutdown of internal services could complete.");
            } else {
                LOG.error("Error while waiting for all services to shut down.", e);
            }
        }
        LOG.info("Leaving.. Bye!");
    }

    public Undertow getServer() {
        return server;
    }

    public RxJsonPersistence getPersistence() {
        return persistence;
    }

    public SagaExecutionCoordinator getSec() {
        return sec;
    }

    public SagaRepository getSagaRepository() {
        return sagaRepository;
    }

    public SagasObserver getSagasObserver() {
        return sagasObserver;
    }

    public SagaLogPool getSagaLogPool() {
        return sagaLogPool;
    }

    public SelectableThreadPoolExectutor getSagaThreadPool() {
        return sagaThreadPool;
    }

    public SagaRecoveryTrigger getSagaRecoveryTrigger() {
        return sagaRecoveryTrigger;
    }

    public Undertow getUndertowServer() {
        return server;
    }

    public Specification getSpecification() {
        return specification;
    }

    public TxlogRawdataPool getTxlogRawdataPool() {
        return txlogRawdataPool;
    }
}
