package no.ssb.lds.core.domain.resource;

import no.ssb.lds.core.specification.Specification;
import no.ssb.lds.core.specification.SpecificationElement;
import no.ssb.lds.core.specification.SpecificationElementType;
import org.json.JSONArray;
import org.json.JSONObject;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.function.Function;
import java.util.function.Supplier;

public class ResourceContext {

    public static ResourceContext createResourceContext(Specification specification, String requestPath) throws ResourceException {
        SpecificationElement specificationRootElement = specification.getRootElement();
        String[] pathParts = requestPath.substring(1).split("/");
        if (pathParts.length < 2) {
            throw new ResourceException("Not a valid resource. The resource path must contain at least namespace, name, and id of managed resource");
        }
        String namespace = urlDecode(pathParts[0]);
        String managedResourceName = urlDecode(pathParts[1]);
        if (!specificationRootElement.getProperties().containsKey(managedResourceName)) {
            throw new ResourceException("Not a managed resource name: \"" + managedResourceName + "\"");
        }
        String managedResourceId = null;
        if (pathParts.length > 2) {
            managedResourceId = urlDecode(pathParts[2]);
            if (managedResourceId.trim().isEmpty()) {
                throw new ResourceException("Managed resource id cannot be empty.");
            }
        }
        SpecificationElement managedSpecificationElement = specificationRootElement.getProperties().get(managedResourceName);
        ResourceElement secondElement = navigateSpecAndCreateResourceContext(managedSpecificationElement, 3, pathParts);
        ResourceElement firstElement = new ResourceElement(managedSpecificationElement, managedResourceName, managedResourceId, secondElement);
        return new ResourceContext(namespace, firstElement);
    }

    private static ResourceElement navigateSpecAndCreateResourceContext(SpecificationElement parentSpecificationElement, int depth, String[] resourcePath) {
        if (depth > (resourcePath.length - 1)) {
            // managed or embedded resource
            return null;
        }
        String pathElement = urlDecode(resourcePath[depth]);
        if (parentSpecificationElement.getProperties() == null
                || !parentSpecificationElement.getProperties().containsKey(pathElement)) {
            throw new ResourceException("Not a valid path element: \"" + pathElement + "\"");
        }
        SpecificationElement matchedElement = parentSpecificationElement.getProperties().get(pathElement);
        if (SpecificationElementType.REF.equals(matchedElement.getSpecificationElementType())) {
            if ((depth + 2) > (resourcePath.length - 1)) {
                if ((depth + 1) > (resourcePath.length - 1)) {
                    // embedded resource with linked-data value(s)
                    return new ResourceElement(matchedElement, pathElement, null, null);
                }
                throw new ResourceException("Reference resource path-element: \"" + pathElement + "\" must be part of pattern ending like: \"/" + pathElement + "/<managed-domain>/<id>\"");
            }
            if ((depth + 2) < (resourcePath.length - 1)) {
                throw new ResourceException("Resource path cannot navigate through REF property: \"" + pathElement + "\"");
            }
            // (depth + 1) == (resourcePath.length - 1)
            // linked-data resource
            String linkedManagedDomain = urlDecode(resourcePath[depth + 1]);
            if (!matchedElement.getRefTypes().contains(linkedManagedDomain)) {
                throw new ResourceException("Reference resource does not support link to managed-domain \"" + linkedManagedDomain + "\". Must be one of: " + matchedElement.getRefTypes());
            }
            String linkedResourceId = urlDecode(resourcePath[depth + 2]);
            String referenceValue = "/" + linkedManagedDomain + "/" + linkedResourceId;
            return new ResourceElement(matchedElement, pathElement, referenceValue, null);
        }
        return new ResourceElement(matchedElement, pathElement, null, navigateSpecAndCreateResourceContext(matchedElement, depth + 1, resourcePath));
    }

    private static ResourceType getResourceType(ResourceElement firstElement) {
        if (!firstElement.hasNext()) {
            return ResourceType.MANAGED;
        }
        ResourceElement lastElement = firstElement.next();
        while (lastElement.hasNext()) {
            lastElement = lastElement.next();
        }
        if (lastElement.id() != null) {
            return ResourceType.REFERENCE;
        }
        return ResourceType.EMBEDDED;
    }

    private final String namespace;
    private final ResourceElement firstElement;
    private final ResourceType resourcetype;

    private ResourceContext(String namespace, ResourceElement firstElement) {
        this.namespace = namespace;
        this.firstElement = firstElement;
        this.resourcetype = getResourceType(firstElement);
    }

    private static String urlDecode(String encoded) {
        return URLDecoder.decode(encoded, StandardCharsets.UTF_8);
    }

    public String getNamespace() {
        return namespace;
    }

    public ResourceElement getFirstElement() {
        return firstElement;
    }

    public boolean isReference() {
        return ResourceType.REFERENCE == resourcetype;
    }

    public boolean isManaged() {
        return ResourceType.MANAGED == resourcetype;
    }

    public boolean isEmbedded() {
        return ResourceType.EMBEDDED == resourcetype;
    }

    static class VisitContext {
        final JSONObject object;
        final ResourceElement element;

        VisitContext(JSONObject object, ResourceElement element) {
            this.object = object;
            this.element = element;
        }
    }

    private <R> R navigateJson(JSONObject rootNode,
                               Function<VisitContext, R> objectTargetFunction,
                               Supplier<R> emptySupplier) {
        JSONObject node = rootNode;
        ResourceElement element = firstElement;
        element = element.next();
        while (element.hasNext()) {
            node = node.getJSONObject(element.name());
            if (node == null) {
                return emptySupplier.get();
            }
            element = element.next();
        }
        return objectTargetFunction.apply(new VisitContext(node, element));
    }

    public boolean referenceToExists(JSONObject rootNode) {
        return navigateJson(rootNode, (vc) -> {
            if (vc.object == null) {
                return false;
            }
            JSONArray jsonArray = vc.object.optJSONArray(vc.element.name());
            if (jsonArray != null) {
                // scan array
                for (int i = 0; i < jsonArray.length(); i++) {
                    String idref = jsonArray.getString(i);
                    if (vc.element.id().equals(idref)) {
                        return true;
                    }
                }
                return false;
            }
            return vc.element.id().equals(vc.object.optString(vc.element.name()));
        }, () -> false);
    }

    /**
     * @param rootNode
     * @return null if content is empty, JSONObject or JSONArray otherwise.
     */
    public Object subTree(JSONObject rootNode) {
        return navigateJson(rootNode, (vc) -> {
            if (vc.object == null) {
                return null;
            }
            JSONArray jsonArray = vc.object.optJSONArray(vc.element.name());
            if (jsonArray != null) {
                return jsonArray;
            }
            Object jsonObject = vc.object.opt(vc.element.name());
            return jsonObject;
        }, () -> null);
    }

    public <R> R navigateAndCreateJson(JSONObject jn, Function<ResourceJsonTraversalTuple, R> f) {
        return navigateAndCreateJson(firstElement, jn, f);
    }

    private <R> R navigateAndCreateJson(ResourceElement re, JSONObject jn, Function<ResourceJsonTraversalTuple, R> f) {
        ResourceElement nextResourceElement = re.next();
        if (!nextResourceElement.hasNext()) {
            return f.apply(new ResourceJsonTraversalTuple(nextResourceElement, jn));
        }
        JSONObject nextJsonNode;
        if (!jn.has(nextResourceElement.name())) {
            if (nextResourceElement.getSpecificationElement().getJsonTypes().contains("array")) {
                JSONArray value = new JSONArray();
                jn.put(nextResourceElement.name(), value);
            } else {
                JSONObject value = new JSONObject();
                jn.put(nextResourceElement.name(), value);
            }
        }
        nextJsonNode = jn.getJSONObject(nextResourceElement.name()); // TODO support JSONArray also
        return navigateAndCreateJson(nextResourceElement, nextJsonNode, f);
    }

}
