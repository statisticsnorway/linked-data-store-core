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
import no.ssb.lds.core.search.SearchIndexConfigurator;
import no.ssb.lds.core.specification.JsonSchemaBasedSpecification;
import no.ssb.lds.graphql.GraphqlHttpHandler;
import no.ssb.lds.graphql.jsonSchema.GraphQLToJsonConverter;
import no.ssb.lds.graphql.schemas.GraphQLSchemaBuilder;
import no.ssb.lds.graphql.schemas.SpecificationConverter;
import no.ssb.rawdata.api.RawdataClient;
import no.ssb.rawdata.api.RawdataClientInitializer;
import no.ssb.sagalog.SagaLogInitializer;
import no.ssb.sagalog.SagaLogPool;
import no.ssb.service.provider.api.ProviderConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
    private final RawdataClient txLogClient;

    UndertowApplication(Specification specification, RxJsonPersistence persistence, SagaExecutionCoordinator sec,
                        SagaRepository sagaRepository, SagasObserver sagasObserver, SagaRecoveryTrigger sagaRecoveryTrigger, String host, int port,
                        SagaLogPool sagaLogPool, SelectableThreadPoolExectutor sagaThreadPool,
                        NamespaceController namespaceController, SearchIndex searchIndex,
                        DynamicConfiguration configuration, RawdataClient txLogClient) {
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
        this.txLogClient = txLogClient;

        String namespace = configuration.evaluateToString("namespace.default");
        boolean enableRequestDump = configuration.evaluateToBoolean("http.request.dump");
        boolean graphqlEnabled = configuration.evaluateToBoolean("graphql.enabled");
        String pathPrefix = configuration.evaluateToString("http.prefix");

        Optional<String> graphQLSchemaPath = Optional.ofNullable(configuration.evaluateToString("graphql.schema"))
                .map(path -> path.isEmpty() ? null : path);


        PathHandler pathHandler = Handlers.path();
        if (graphqlEnabled) {

            GraphQLSchemaBuilder schemaBuilder = new GraphQLSchemaBuilder(namespace, persistence, searchIndex);
            TypeDefinitionRegistry definitionRegistry;
            if (graphQLSchemaPath.isPresent()) {
                File graphQLFile = new File(graphQLSchemaPath.get());
                definitionRegistry = new SchemaParser().parse(graphQLFile);
            } else {
                SpecificationConverter specificationConverter = new SpecificationConverter();
                definitionRegistry = specificationConverter.convert(specification);
            }
            GraphQLSchema schema = schemaBuilder.getGraphQL(GraphQLSchemaBuilder.parseSchema(definitionRegistry));

            GraphQL graphQL = GraphQL.newGraphQL(schema).build();

            GraphQLToJsonConverter graphQLToJsonConverter = new GraphQLToJsonConverter(schema);
            graphQLToJsonConverter.parseGraphQLSchema(schema);

            pathHandler.addExactPath("/graphiql", Handlers.resource(new ClassPathResourceManager(
                    Thread.currentThread().getContextClassLoader(), "no/ssb/lds/graphql/graphiql"
            )).setDirectoryListingEnabled(false).addWelcomeFiles("graphiql.html"));

            GraphqlHttpHandler graphqlHttpHandler = new GraphqlHttpHandler(graphQL);
            pathHandler.addExactPath("/graphql", graphqlHttpHandler);
        }


        HealthCheckHandler healthHandler = new HealthCheckHandler(persistence);
        pathHandler.addExactPath(HealthCheckHandler.HEALTH_ALIVE_PATH, healthHandler);
        ResponseCodeHandler aliveHandler = new ResponseCodeHandler(StatusCodes.OK);
        pathHandler.addExactPath(HealthCheckHandler.HEALTH_READY_PATH, aliveHandler);
        pathHandler.addExactPath(HealthCheckHandler.PING_PATH, aliveHandler);
        pathHandler.addPrefixPath("/", namespaceController);

        HttpHandler httpHandler;
        if (enableRequestDump) {
            httpHandler = Handlers.requestDump(pathHandler);
        } else {
            httpHandler = pathHandler;
        }

        if (pathPrefix != null && !pathPrefix.isEmpty()) {
            LOG.info("Using http prefix: {}", pathPrefix);
            httpHandler = Handlers.path(ResponseCodeHandler.HANDLE_404).addPrefixPath(pathPrefix, httpHandler);
        }

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

        this.server = Undertow.builder()
                .addHttpListener(port, host)
                .setHandler(corsHandler)
                .build();
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
        String schemaConfigStr = configuration.evaluateToString("specification.schema");
        String[] specificationSchema = ("".equals(schemaConfigStr) ? new String[0] : schemaConfigStr.split(","));
        JsonSchemaBasedSpecification specification = JsonSchemaBasedSpecification.create(specificationSchema);
        RxJsonPersistence persistence = PersistenceConfigurator.configurePersistence(configuration, specification);

        ServiceLoader<SagaLogInitializer> loader = ServiceLoader.load(SagaLogInitializer.class);
        String sagalogProviderClass = configuration.evaluateToString("sagalog.provider");
        SagaLogPool sagaLogPool = loader.stream().filter(c -> sagalogProviderClass.equals(c.type().getName())).findFirst().orElseThrow().get().initialize(configuration.asMap());

        String host = configuration.evaluateToString("http.host");
        SagaRepository.Builder sagaRepositoryBuilder = new SagaRepository.Builder()
                .specification(specification)
                .persistence(persistence);
        SearchIndex searchIndex = SearchIndexConfigurator.configureSearchIndex(configuration);
        if (searchIndex != null) {
            sagaRepositoryBuilder.indexer(searchIndex);
        }
        RawdataClient txLogClient = configureTxLogRawdataClient(configuration);
        if (txLogClient != null) {
            String txLogTopic = configuration.evaluateToString("txlog.rawdata.topic");
            sagaRepositoryBuilder.txLogClient(txLogClient).txLogTopic(txLogTopic);
        }
        SagaRepository sagaRepository = sagaRepositoryBuilder.build();
        final SagasObserver sagasObserver = new SagasObserver(sagaRepository).start();
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

        AtomicInteger executorThreadId = new AtomicInteger();
        ScheduledExecutorService recoveryThreadPool = Executors.newScheduledThreadPool(10, runnable -> new Thread(runnable, "saga-recovery-" + executorThreadId.incrementAndGet()));

        SagaExecutionCoordinator sec = new SagaExecutionCoordinator(sagaLogPool, numberOfSagaLogs, sagaRepository, sagasObserver, sagaThreadPool, sagaCommandsEnabled, recoveryThreadPool);

        HystrixThreadPoolProperties.Setter().withMaximumSize(50); // TODO Configure Hystrix properly

        boolean sagaRecoveryEnabled = configuration.evaluateToBoolean("saga.recovery.enabled");
        SagaRecoveryTrigger sagaRecoveryTrigger = null;
        if (sagaRecoveryEnabled) {
            int intervalMinSec = configuration.evaluateToInt("saga.recovery.interval.seconds.min");
            int intervalMaxSec = configuration.evaluateToInt("saga.recovery.interval.seconds.max");
            sagaRecoveryTrigger = new SagaRecoveryTrigger(sec, intervalMinSec, intervalMaxSec, recoveryThreadPool);
        }

        NamespaceController namespaceController = new NamespaceController(
                configuration.evaluateToString("namespace.default"),
                specification,
                specification,
                persistence,
                sec,
                sagaRepository
        );

        return new UndertowApplication(specification, persistence, sec, sagaRepository, sagasObserver, sagaRecoveryTrigger, host, port,
                sagaLogPool, sagaThreadPool, namespaceController,
                searchIndex, configuration, txLogClient);
    }

    public static RawdataClient configureTxLogRawdataClient(DynamicConfiguration configuration) {
        String txLogRawdataProvider = configuration.evaluateToString("txlog.rawdata.provider");
        NavigableMap<String, String> configMap = new TreeMap<>(configuration.asMap());
        String txLogRawdataProviderConfigPrefix = "txlog.rawdata." + txLogRawdataProvider + ".";
        NavigableMap<String, String> txLogRawdataProviderConfigWithPrefix = configMap.subMap(
                txLogRawdataProviderConfigPrefix, true,
                txLogRawdataProviderConfigPrefix + "~", false);
        Map<String, String> txLogRawdataProviderConfig = txLogRawdataProviderConfigWithPrefix.entrySet().stream().collect(Collectors.toMap(
                e -> e.getKey().substring(txLogRawdataProviderConfigPrefix.length()),
                e -> e.getValue())
        );
        return ProviderConfigurator.configure(txLogRawdataProviderConfig, txLogRawdataProvider, RawdataClientInitializer.class);
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
        server.start();
        if (sagaRecoveryTrigger != null) {
            // attempt to recover local saga-logs immediately, then attempt cluster wide recovery regularly
            sec.completeLocalIncompleteSagas(sec.getRecoveryThreadPool()).handle((v, t) -> {
                if (t != null) {
                    LOG.error("Error during initial local saga-logs recovery attempt", t);
                }
                sagaRecoveryTrigger.start();
                return (Void) null;
            });
        }
        LOG.info("Started Linked Data Store server. PID {}", ProcessHandle.current().pid());
        LOG.info("Listening on {}:{}", host, port);
    }

    public void stop() {
        server.stop();
        persistence.close();
        try {
            txLogClient.close();
        } catch (Error | RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        sagasObserver.shutdown();
        if (sagaRecoveryTrigger != null) {
            sagaRecoveryTrigger.stop();
        }
        sec.shutdown();
        shutdownAndAwaitTermination(sagaThreadPool);
        sagaLogPool.shutdown();
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

    public RawdataClient getTxLogClient() {
        return txLogClient;
    }
}
