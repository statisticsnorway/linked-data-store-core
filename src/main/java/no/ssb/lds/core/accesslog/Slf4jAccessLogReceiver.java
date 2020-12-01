package no.ssb.lds.core.accesslog;

import io.undertow.server.handlers.accesslog.AccessLogReceiver;
import org.slf4j.Logger;

public class Slf4jAccessLogReceiver implements AccessLogReceiver {
    private final Logger logger;

    public Slf4jAccessLogReceiver(final Logger logger) {
        this.logger = logger;
    }

    @Override
    public void logMessage(String message) {
        logger.info(message);
    }
}