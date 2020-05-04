import no.ssb.lds.api.persistence.PersistenceInitializer;
import no.ssb.lds.api.search.SearchIndexProvider;
import no.ssb.rawdata.api.RawdataClientInitializer;
import no.ssb.sagalog.SagaLogInitializer;

module no.ssb.lds.core {
    requires no.ssb.lds.persistence.api;
    requires no.ssb.lds.search.api;
    requires no.ssb.config;
    requires no.ssb.concurrent.futureselector;
    requires no.ssb.saga.api;
    requires no.ssb.saga.execution;
    requires no.ssb.sagalog;
    requires no.ssb.rawdata.api;
    requires de.huxhorn.sulky.ulid;
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
    requires jackson.dataformat.msgpack;

    requires graphql.java;
    requires io.reactivex.rxjava2;
    requires org.reactivestreams;
    requires com.github.akarnokd.rxjava2jdk9interop;
    requires graphql.java.extended.scalars;

    opens no.ssb.lds.graphql.graphiql;

    provides SearchIndexProvider with no.ssb.lds.core.search.TestSearchIndex;

    uses PersistenceInitializer;
    uses RawdataClientInitializer;
    uses SearchIndexProvider;
    uses SagaLogInitializer;

    exports no.ssb.lds.core;
    exports no.ssb.lds.test.server; // Needed to run tests in IntelliJ
}
