package no.ssb.lds.core.domain.reference;

import no.ssb.lds.api.specification.Specification;
import no.ssb.lds.api.specification.SpecificationElement;
import no.ssb.lds.core.domain.resource.ResourceContext;
import no.ssb.lds.core.domain.resource.ResourceElement;
import org.json.JSONArray;
import org.json.JSONObject;

public class ReferenceJsonHelper {

    final Specification specification;
    final ResourceElement resourceElement;
    final SpecificationElement root;

    public ReferenceJsonHelper(Specification specification, ResourceElement resourceElement) {
        this.specification = specification;
        this.resourceElement = resourceElement;
        this.root = specification.getRootElement();
    }

    public boolean deleteReferenceJson(ResourceContext resourceContext, JSONObject rootNode) {
        return resourceContext.navigateAndCreateJson(rootNode, t -> {
            String referencePropertyName = t.resourceElement.name();
            String referenceValue = t.resourceElement.id();
            if (t.resourceElement.getSpecificationElement().getJsonTypes().contains("array")) {
                JSONObject refParentNode = t.jsonObject;
                if (!refParentNode.has(referencePropertyName)) {
                    return false;
                }
                JSONArray refNode = (JSONArray) refParentNode.get(referencePropertyName);
                for (int i = 0; i < refNode.length(); i++) {
                    String idref = refNode.getString(i);
                    if (referenceValue.equals(idref)) {
                        refNode.remove(i);
                        return true;
                    }
                }
                return false; // not found
            } else {
                JSONObject refParentNode = t.jsonObject;
                if (referenceValue.equals(refParentNode.getString(referencePropertyName))) {
                    refParentNode.remove(referencePropertyName);
                    return true;
                }
                return false;
            }
        });
    }

    public boolean createReferenceJson(ResourceContext resourceContext, JSONObject rootNode) {
        return resourceContext.navigateAndCreateJson(rootNode, t -> {
            String referencePropertyName = t.resourceElement.name();
            String referenceValue = t.resourceElement.id();
            if (t.resourceElement.getSpecificationElement().getJsonTypes().contains("array")) {
                JSONObject refParentNode = t.jsonObject;
                if (!refParentNode.has(referencePropertyName)) {
                    refParentNode.put(referencePropertyName, new JSONArray());
                }
                JSONArray refNode = (JSONArray) refParentNode.get(referencePropertyName);
                refNode.put(referenceValue);
                return true;
            } else {
                JSONObject refParentNode = t.jsonObject;
                refParentNode.put(referencePropertyName, referenceValue);
                return true;
            }
        });
    }
}
