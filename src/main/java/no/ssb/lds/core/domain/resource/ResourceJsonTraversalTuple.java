package no.ssb.lds.core.domain.resource;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class ResourceJsonTraversalTuple {

    public final ResourceElement resourceElement;
    public final ObjectNode jsonObject;

    ResourceJsonTraversalTuple(ResourceElement resourceElement, JsonNode jsonObject) {
        this.resourceElement = resourceElement;
        // TODO Use JsonNode type and support array-navigation
        if (!jsonObject.isObject()) {
            throw new UnsupportedOperationException("array-navigation not supported");
        }
        this.jsonObject = (ObjectNode) jsonObject;
    }
}
