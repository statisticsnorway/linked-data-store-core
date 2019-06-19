package no.ssb.lds.core.saga;

import io.undertow.server.HttpServerExchange;

import java.util.Collections;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class SagaCommands {

    public static Map<String, List<SagaCommand>> getSagaAdminParameterCommands(HttpServerExchange exchange) {
        Deque<String> sagaCommands = exchange.getQueryParameters().get("saga");
        if (sagaCommands == null) {
            return Collections.emptyMap();
        }
        Map<String, List<SagaCommand>> map = new LinkedHashMap<>();
        for (String sagaCommand : sagaCommands) {
            String[] parts = sagaCommand.split("\\s");
            if (parts.length < 2) {
                continue;
            }
            String command = parts[0];
            String nodeId = parts[1];
            List<String> args = new LinkedList<>();
            for (int i = 2; i < parts.length; i++) {
                args.add(parts[i]);
            }
            map.computeIfAbsent(nodeId, n -> new LinkedList<>()).add(new SagaCommand(nodeId, command, args));
        }
        return map;
    }
}
