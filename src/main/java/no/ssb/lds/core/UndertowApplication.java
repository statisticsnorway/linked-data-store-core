package no.ssb.lds.core;

import com.netflix.hystrix.HystrixThreadPoolProperties;
import graphql.GraphQL;
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
import no.ssb.lds.core.saga.FileSagaLog;
import no.ssb.lds.core.saga.SagaExecutionCoordinator;
import no.ssb.lds.core.saga.SagaLogInitializer;
import no.ssb.lds.core.saga.SagaRepository;
import no.ssb.lds.core.saga.SagasObserver;
import no.ssb.lds.core.search.SearchIndexConfigurator;
import no.ssb.lds.core.specification.JsonSchemaBasedSpecification;
import no.ssb.lds.graphql.GraphqlHttpHandler;
import no.ssb.lds.graphql.schemas.OldGraphqlSchemaBuilder;
import no.ssb.saga.execution.sagalog.SagaLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class UndertowApplication {

    private static final Logger LOG = LoggerFactory.getLogger(UndertowApplication.class);

    public static String getDefaultConfigurationResourcePath() {
        return "application-defaults.properties";
    }

    public static UndertowApplication initializeUndertowApplication(DynamicConfiguration configuration) {
        int port = configuration.evaluateToInt("http.port");
        return initializeUndertowApplication(configuration, port);
    }

    private final RxJsonPersistence persistence;

    private final Specification specification;
    private final Undertow server;
    private final String host;
    private final int port;

    UndertowApplication(Specification specification, RxJsonPersistence persistence, SagaExecutionCoordinator sec,
                        SagaRepository sagaRepository, SagasObserver sagasObserver, String host, int port,
                        SagaLog sagaLog, SelectableThreadPoolExectutor sagaThreadPool,
                        NamespaceController namespaceController, SearchIndex searchIndex,
                        DynamicConfiguration configuration) {
        this.specification = specification;
        this.host = host;
        this.port = port;
        this.persistence = persistence;
        this.sec = sec;
        this.sagaRepository = sagaRepository;
        this.sagasObserver = sagasObserver;
        this.sagaLog = sagaLog;
        this.sagaThreadPool = sagaThreadPool;

        String namespace = configuration.evaluateToString("namespace.default");
        boolean enableRequestDump = configuration.evaluateToBoolean("http.request.dump");
        boolean graphqlEnabled = configuration.evaluateToBoolean("graphql.enabled");
        String pathPrefix = configuration.evaluateToString("http.prefix");

        PathHandler pathHandler = Handlers.path();
        if (graphqlEnabled) {
            GraphQL graphQL = GraphQL.newGraphQL(new OldGraphqlSchemaBuilder(specification, persistence, searchIndex, namespace)
                    .getSchema()).build();

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

    private final SagaExecutionCoordinator sec;
    private final SagaRepository sagaRepository;
    private final SagasObserver sagasObserver;
    private final SagaLog sagaLog;
    private final SelectableThreadPoolExectutor sagaThreadPool;

    public static UndertowApplication initializeUndertowApplication(DynamicConfiguration configuration, int port) {
        LOG.info("Initializing Linked Data Store (LDS) server ...");
        String schemaConfigStr = configuration.evaluateToString("specification.schema");
        String[] specificationSchema = ("".equals(schemaConfigStr) ? new String[0] : schemaConfigStr.split(","));
        JsonSchemaBasedSpecification specification = JsonSchemaBasedSpecification.create(specificationSchema);
        RxJsonPersistence persistence = PersistenceConfigurator.configurePersistence(configuration, specification);
        SagaLog sagaLog = SagaLogInitializer.initializeSagaLog(configuration.evaluateToString("saga.log.type"), configuration.evaluateToString("saga.log.type.file.path"));
        String host = configuration.evaluateToString("http.host");
        SearchIndex searchIndex = SearchIndexConfigurator.configureSearchIndex(configuration);
        SagaRepository sagaRepository;
        if (searchIndex != null) {
            sagaRepository = new SagaRepository(specification, persistence, searchIndex);
        } else {
            sagaRepository = new SagaRepository(specification, persistence);
        }
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
        SagaExecutionCoordinator sec = new SagaExecutionCoordinator(sagaLog, sagaRepository, sagasObserver, sagaThreadPool);

        HystrixThreadPoolProperties.Setter().withMaximumSize(50); // TODO Configure Hystrix properly

        NamespaceController namespaceController = new NamespaceController(
                configuration.evaluateToString("namespace.default"),
                specification,
                specification,
                persistence,
                sec,
                sagaRepository
        );

        return new UndertowApplication(specification, persistence, sec, sagaRepository, sagasObserver, host, port,
                sagaLog, sagaThreadPool, namespaceController,
                searchIndex, configuration);
    }

    public void enableSagaExecutionAutomaticDeadlockDetectionAndResolution() {
        sec.startThreadpoolWatchdog();
    }

    public void triggerRecoveryOfIncompleteSagas() {
        sec.recoverIncompleteSagas();
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public void start() {
        server.start();
        LOG.info("Started Linked Data Store server. PID {}", ProcessHandle.current().pid());
        LOG.info("Listening on {}:{}", host, port);
    }

    public void stop() {
        server.stop();
        persistence.close();
        sagasObserver.shutdown();
        sec.shutdown();
        shutdownAndAwaitTermination(sagaThreadPool);
        if (sagaLog instanceof FileSagaLog) {
            ((FileSagaLog) sagaLog).close();
        }
        LOG.info("Leaving.. Bye!");
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

    public SagaLog getSagaLog() {
        return sagaLog;
    }

    public SelectableThreadPoolExectutor getSagaThreadPool() {
        return sagaThreadPool;
    }

    public Undertow getUndertowServer() {
        return server;
    }

    public Specification getSpecification() {
        return specification;
    }
}
