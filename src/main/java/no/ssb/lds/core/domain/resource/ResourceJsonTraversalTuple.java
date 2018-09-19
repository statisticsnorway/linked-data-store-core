package no.ssb.lds.core.domain.resource;

import org.json.JSONObject;

public class ResourceJsonTraversalTuple {

    public final ResourceElement resourceElement;
    public final JSONObject jsonObject;

    ResourceJsonTraversalTuple(ResourceElement resourceElement, JSONObject jsonObject) {
        this.resourceElement = resourceElement;
        this.jsonObject = jsonObject;
    }
}
