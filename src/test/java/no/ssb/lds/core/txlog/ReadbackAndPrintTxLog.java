package no.ssb.lds.core.txlog;

import com.fasterxml.jackson.databind.ObjectMapper;
import no.ssb.config.DynamicConfiguration;
import no.ssb.config.StoreBasedDynamicConfiguration;
import no.ssb.lds.api.persistence.json.JsonTools;
import no.ssb.lds.core.UndertowApplication;
import no.ssb.rawdata.api.RawdataClient;
import no.ssb.rawdata.api.RawdataConsumer;
import no.ssb.rawdata.api.RawdataMessage;
import org.msgpack.jackson.dataformat.MessagePackFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class ReadbackAndPrintTxLog {

    public static void main(String[] args) throws Exception {
        DynamicConfiguration configuration = new StoreBasedDynamicConfiguration.Builder()
                .propertiesResource("application_test.properties")
                .build();
        readDumpTxLog(configuration);
    }

    public static void readDumpTxLog(DynamicConfiguration configuration) throws Exception {
        try (RawdataClient rawdataClient = UndertowApplication.configureTxLogRawdataClient(configuration)) {
            String txLogTopic = configuration.evaluateToString("txlog.rawdata.topic");
            try (RawdataConsumer consumer = rawdataClient.consumer(txLogTopic)) {
                ObjectMapper objectMapper = new ObjectMapper(new MessagePackFactory());
                for (int i = 1; ; i++) {
                    RawdataMessage message = consumer.receive(0, TimeUnit.MILLISECONDS);
                    if (message == null) {
                        break;
                    }
                    String content = message.keys().stream().map(k -> {
                        try {
                            return k + " " + JsonTools.toJson(objectMapper.readTree(message.get(k)));
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }).collect(Collectors.joining(", "));
                    System.out.printf("%d %s  CONTENT: %s%n", i, message, content);
                }
            }
        }
    }
}
