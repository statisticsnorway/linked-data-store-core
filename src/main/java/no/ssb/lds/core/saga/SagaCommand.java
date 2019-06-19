package no.ssb.lds.core.saga;

import java.util.List;

public class SagaCommand {
    private final String nodeId;
    private final String command;
    private final List<String> arguments;

    public SagaCommand(String nodeId, String command, List<String> arguments) {
        this.nodeId = nodeId;
        this.command = command;
        this.arguments = arguments;
    }

    public String getNodeId() {
        return nodeId;
    }

    public String getCommand() {
        return command;
    }

    public List<String> getArguments() {
        return arguments;
    }
}
