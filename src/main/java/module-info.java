import no.ssb.lds.api.persistence.PersistenceInitializer;
import no.ssb.lds.core.extension.SearchIndexProvider;

module no.ssb.lds.core {
    requires no.ssb.lds.persistence.api;
    requires no.ssb.config;
    requires no.ssb.concurrent.futureselector;
    requires no.ssb.saga.api;
    requires no.ssb.saga.execution;
    requires jdk.unsupported;
    requires java.base;
    requires java.net.http;
    requires org.slf4j;
    requires undertow.core;
    requires xnio.api;
    requires org.json;
    requires hystrix.core;
    requires org.everit.json.schema;
    requires java.xml; // TODO this should be in test-scope only!
    requires com.fasterxml.jackson.core;
    requires com.fasterxml.jackson.databind;

    requires graphql.java;
    requires io.reactivex.rxjava2;
    requires org.reactivestreams;
    requires com.github.akarnokd.rxjava2jdk9interop;

    opens no.ssb.lds.graphql.graphiql;

    uses PersistenceInitializer;
    uses SearchIndexProvider;

    exports no.ssb.lds.core;
}
