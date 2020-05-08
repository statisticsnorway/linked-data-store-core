package no.ssb.lds.core.txlog;

import no.ssb.rawdata.api.RawdataClient;
import no.ssb.rawdata.api.RawdataMessage;
import no.ssb.rawdata.api.RawdataProducer;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Optional.ofNullable;

public class TxlogRawdataPool {

    private final RawdataClient client;
    private final boolean splitSources;
    private final String defaultSource;
    private final String txLogTopicPrefix;

    private final Map<String, RawdataProducer> producerByTopic = new ConcurrentHashMap<>();

    public TxlogRawdataPool(RawdataClient client, boolean splitSources, String defaultSource, String txLogTopicPrefix) {
        this.client = client;
        this.splitSources = splitSources;
        this.defaultSource = defaultSource;
        this.txLogTopicPrefix = txLogTopicPrefix;
    }

    public RawdataClient getClient() {
        return client;
    }

    public boolean isSplitSources() {
        return splitSources;
    }

    public String getDefaultSource() {
        return defaultSource;
    }

    public String getTxLogTopicPrefix() {
        return txLogTopicPrefix;
    }

    public RawdataProducer producer(String source) {
        String topic = topicOf(source);
        return producerByTopic.computeIfAbsent(topic, client::producer);
    }

    public String topicOf(String source) {
        if (splitSources) {
            // Use separate tx-log topic per source
            return txLogTopicPrefix + ofNullable(source).orElse(defaultSource);
        }
        // Always use default source
        return txLogTopicPrefix + defaultSource;
    }

    public RawdataMessage getLastMessage(String source) {
        String topic = topicOf(source);
        return client.lastMessage(topic);
    }
}
