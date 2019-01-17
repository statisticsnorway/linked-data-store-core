package no.ssb.lds.core;

import com.netflix.hystrix.HystrixThreadPoolProperties;
import graphql.GraphQL;
import io.undertow.Handlers;
import io.undertow.Undertow;
import io.undertow.server.HttpHandler;
import io.undertow.server.RoutingHandler;
import io.undertow.server.handlers.resource.ClassPathResourceManager;
import no.ssb.concurrent.futureselector.SelectableThreadPoolExectutor;
import no.ssb.config.DynamicConfiguration;
import no.ssb.lds.api.persistence.json.JsonPersistence;
import no.ssb.lds.api.specification.Specification;
import no.ssb.lds.core.controller.NamespaceController;
import no.ssb.lds.core.persistence.PersistenceConfigurator;
import no.ssb.lds.core.saga.FileSagaLog;
import no.ssb.lds.core.saga.SagaExecutionCoordinator;
import no.ssb.lds.core.saga.SagaLogInitializer;
import no.ssb.lds.core.saga.SagaRepository;
import no.ssb.lds.core.saga.SagasObserver;
import no.ssb.lds.core.specification.JsonSchemaBasedSpecification;
import no.ssb.lds.graphql.GraphqlHttpHandler;
import no.ssb.lds.graphql.schemas.GraphqlSchemaBuilder;
import no.ssb.saga.execution.sagalog.SagaLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class UndertowApplication {

    private static final Logger LOG = LoggerFactory.getLogger(UndertowApplication.class);

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
        JsonPersistence persistence = PersistenceConfigurator.configurePersistence(configuration, specification);
        SagaLog sagaLog = SagaLogInitializer.initializeSagaLog(configuration.evaluateToString("saga.log.type"), configuration.evaluateToString("saga.log.type.file.path"));
        String host = configuration.evaluateToString("http.host");
        SagaRepository sagaRepository = new SagaRepository(specification, persistence);
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
                configuration.evaluateToString("http.cors.allow.origin"),
                configuration.evaluateToString("http.cors.allow.header"),
                configuration.evaluateToBoolean("http.cors.allow.origin.test"),
                sec,
                sagaRepository,
                port
        );

        boolean graphqlEnabled = configuration.evaluateToBoolean("graphql.enabled");

        // TODO: Pass configuration instead to avoid so many parameters. Undertow has a nice builder pattern.
        return new UndertowApplication(specification, persistence, sec, sagaRepository, sagasObserver, host, port,
                sagaLog, sagaThreadPool, namespaceController, graphqlEnabled,
                configuration.evaluateToString("namespace.default"));
    }

    private final Specification specification;
    private final Undertow server;
    private final String host;
    private final int port;
    private final JsonPersistence persistence;
    private final SagaExecutionCoordinator sec;
    private final SagaRepository sagaRepository;
    private final SagasObserver sagasObserver;
    private final SagaLog sagaLog;
    private final SelectableThreadPoolExectutor sagaThreadPool;

    UndertowApplication(Specification specification, JsonPersistence persistence, SagaExecutionCoordinator sec,
                        SagaRepository sagaRepository, SagasObserver sagasObserver, String host, int port,
                        SagaLog sagaLog, SelectableThreadPoolExectutor sagaThreadPool,
                        NamespaceController namespaceController, boolean graphqlEnabled, String nameSpace) {
        this.specification = specification;
        this.host = host;
        this.port = port;
        this.persistence = persistence;
        this.sec = sec;
        this.sagaRepository = sagaRepository;
        this.sagasObserver = sagasObserver;
        this.sagaLog = sagaLog;
        this.sagaThreadPool = sagaThreadPool;

        // TODO: Clean up.
        RoutingHandler routingHandler = Handlers.routing();

        if (graphqlEnabled) {
            GraphQL graphQL = GraphQL.newGraphQL(new GraphqlSchemaBuilder(specification, persistence, nameSpace)
                    .getSchema()).build();
            routingHandler = routingHandler
                    .get("graphiql**", Handlers.resource(new ClassPathResourceManager(
                                    Thread.currentThread().getContextClassLoader(), "no/ssb/lds/graphql"
                            )).setDirectoryListingEnabled(false).addWelcomeFiles("graphiql.html")
                    )
                    .post("graphql", new GraphqlHttpHandler(graphQL));
        }

        HttpHandler httpHandler = routingHandler
                .setFallbackHandler(namespaceController);

        httpHandler = Handlers.requestDump(httpHandler);

        this.server = Undertow.builder()
                .addHttpListener(port, host)
                .setHandler(httpHandler)
                .build();
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

    public JsonPersistence getPersistence() {
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
