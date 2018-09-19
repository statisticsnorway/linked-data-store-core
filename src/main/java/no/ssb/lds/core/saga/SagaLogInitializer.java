package no.ssb.lds.core.saga;

import no.ssb.saga.execution.sagalog.SagaLog;
import no.ssb.saga.execution.sagalog.SagaLogEntry;

import java.io.File;
import java.util.Collections;
import java.util.List;

public class SagaLogInitializer {

    public static SagaLog initializeSagaLog(String sagaLogType, String sagaLogFilePath) {
        SagaLog sagaLog;
        if ("file".equals(sagaLogType)) {
            sagaLog = new FileSagaLog(new File(sagaLogFilePath), -1);
        } else if ("none".equals(sagaLogType)) {
            sagaLog = new SagaLog() {
                @Override
                public String write(SagaLogEntry entry) {
                    return "{\"logid\":\"" + System.currentTimeMillis() + "\"}";
                }

                @Override
                public List<SagaLogEntry> readEntries(String executionId) {
                    return Collections.emptyList();
                }
            };
        } else if ("distributedlog".equals(sagaLogType)) {
            sagaLog = new DistributedLogHttpProxyClient("http://localhost:8008/");
        } else {
            throw new IllegalStateException("Configuration 'saga.log.type' must be one of ('file', 'none', 'distributedlog')");
        }
        return sagaLog;
    }
}
