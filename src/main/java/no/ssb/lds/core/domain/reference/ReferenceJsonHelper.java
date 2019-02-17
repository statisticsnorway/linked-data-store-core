package no.ssb.lds.core.domain.reference;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import no.ssb.lds.api.specification.Specification;
import no.ssb.lds.api.specification.SpecificationElement;
import no.ssb.lds.core.domain.resource.ResourceContext;
import no.ssb.lds.core.domain.resource.ResourceElement;

import java.util.Iterator;

public class ReferenceJsonHelper {

    final Specification specification;
    final ResourceElement resourceElement;
    final SpecificationElement root;

    public ReferenceJsonHelper(Specification specification, ResourceElement resourceElement) {
        this.specification = specification;
        this.resourceElement = resourceElement;
        this.root = specification.getRootElement();
    }

    public boolean deleteReferenceJson(ResourceContext resourceContext, JsonNode rootNode) {
        // TODO Support array-navigation
        return resourceContext.navigateAndCreateJson(rootNode, t -> {
            String referencePropertyName = t.resourceElement.name();
            String referenceValue = t.resourceElement.id();
            if (t.resourceElement.getSpecificationElement().getJsonTypes().contains("array")) {
                ObjectNode refParentNode = t.jsonObject;
                if (!refParentNode.has(referencePropertyName)) {
                    return false;
                }
                ArrayNode refNode = (ArrayNode) refParentNode.get(referencePropertyName);
                for (int i = 0; i < refNode.size(); i++) {
                    String idref = refNode.get(i).textValue();
                    if (referenceValue.equals(idref)) {
                        refNode.remove(i);
                        return true;
                    }
                }
                return false; // not found
            } else {
                ObjectNode refParentNode = t.jsonObject;
                String existingValue = refParentNode.get(referencePropertyName).textValue();
                if (referenceValue.equals(existingValue)) {
                    refParentNode.remove(referencePropertyName);
                    return true;
                }
                return false;
            }
        });
    }

    public boolean createReferenceJson(ResourceContext resourceContext, JsonNode rootNode) {
        // TODO Support array-navigation
        return resourceContext.navigateAndCreateJson(rootNode, t -> {
            String referencePropertyName = t.resourceElement.name();
            String referenceValue = t.resourceElement.id();
            if (t.resourceElement.getSpecificationElement().getJsonTypes().contains("array")) {
                ObjectNode refParentNode = t.jsonObject;
                if (!refParentNode.has(referencePropertyName)) {
                    refParentNode.putArray(referencePropertyName);
                }
                ArrayNode refNode = (ArrayNode) refParentNode.get(referencePropertyName);
                // scan array for existing identical reference value
                Iterator<JsonNode> it = refNode.elements();
                while (it.hasNext()) {
                    JsonNode next = it.next();
                    if (next.isNull()) {
                        continue;
                    }
                    if (referenceValue.equals(next.textValue())) {
                        // value already in array
                        return false;
                    }
                }
                refNode.add(referenceValue);
                return true;
            } else {
                ObjectNode refParentNode = t.jsonObject;
                if (!refParentNode.has(referencePropertyName)
                        || refParentNode.get(referencePropertyName).isNull()) {
                    // not already set
                    refParentNode.put(referencePropertyName, referenceValue);
                    return true;
                } else if (!refParentNode.get(referencePropertyName).textValue().equals(referenceValue)) {
                    // change value
                    refParentNode.put(referencePropertyName, referenceValue);
                    return true;
                }
                // already set to referenceValue value
                return false;
            }
        });
    }
}
