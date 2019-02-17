package no.ssb.lds.core.domain.resource;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import no.ssb.lds.api.specification.Specification;
import no.ssb.lds.api.specification.SpecificationElement;
import no.ssb.lds.api.specification.SpecificationElementType;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.ZonedDateTime;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.util.Optional.ofNullable;

/**
 * TODO Support json array-navigation
 */
public class ResourceContext {

    public static ResourceContext createResourceContext(Specification specification, String requestPath, ZonedDateTime timestamp) throws ResourceException {
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
        return new ResourceContext(namespace, firstElement, timestamp);
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
    private final ZonedDateTime timestamp;

    private ResourceContext(String namespace, ResourceElement firstElement, ZonedDateTime timestamp) {
        this.namespace = namespace;
        this.firstElement = firstElement;
        this.resourcetype = getResourceType(firstElement);
        this.timestamp = timestamp;
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

    public ZonedDateTime getTimestamp() {
        return timestamp;
    }

    static class VisitContext {
        final JsonNode node;
        final ResourceElement element;

        VisitContext(JsonNode node, ResourceElement element) {
            this.node = node;
            this.element = element;
        }
    }

    private <R> R navigateJson(JsonNode rootNode,
                               Function<VisitContext, R> objectTargetFunction,
                               Supplier<R> emptySupplier) {
        JsonNode node = rootNode;
        ResourceElement element = firstElement;
        element = element.next();
        // TODO Support array-navigation
        while (element.hasNext()) {
            node = node.get(element.name());
            if (node == null) {
                return emptySupplier.get();
            }
            element = element.next();
        }
        return objectTargetFunction.apply(new VisitContext(node, element));
    }

    public boolean referenceToExists(JsonNode rootNode) {
        return navigateJson(rootNode, (vc) -> {
            if (vc.node == null) {
                return false;
            }
            JsonNode node = vc.node.get(vc.element.name());
            if (node != null && node.isArray()) {
                // scan array
                ArrayNode arrayNode = (ArrayNode) node;
                for (int i = 0; i < arrayNode.size(); i++) {
                    String idref = arrayNode.get(i).textValue();
                    if (vc.element.id().equals(idref)) {
                        return true;
                    }
                }
                return false;
            }
            return vc.element.id().equals(ofNullable(vc.node.get(vc.element.name())).map(JsonNode::textValue).orElse(null));
        }, () -> false);
    }

    /**
     * @param rootNode
     * @return null if content is empty, the JsonNode instance representing the root of the sub-tree otherwise.
     */
    public JsonNode subTree(JsonNode rootNode) {
        return navigateJson(rootNode, (vc) -> {
            if (vc.node == null) {
                return null;
            }
            if (!vc.node.isContainerNode()) {
                throw new RuntimeException("");
            }
            if (vc.node.isObject()) {
                return vc.node.get(vc.element.name());
            } else {
                // array
                throw new UnsupportedOperationException("array-navigation not supported"); // TODO
            }
        }, () -> null);
    }

    public <R> R navigateAndCreateJson(JsonNode jn, Function<ResourceJsonTraversalTuple, R> f) {
        return navigateAndCreateJson(firstElement, jn, f);
    }

    private <R> R navigateAndCreateJson(ResourceElement re, JsonNode jn, Function<ResourceJsonTraversalTuple, R> f) {
        // TODO support array-navigation
        ResourceElement nextResourceElement = re.next();
        if (!nextResourceElement.hasNext()) {
            return f.apply(new ResourceJsonTraversalTuple(nextResourceElement, jn));
        }
        if (!jn.has(nextResourceElement.name())) {
            ObjectNode oj = (ObjectNode) jn;
            if (nextResourceElement.getSpecificationElement().getJsonTypes().contains("array")) {
                oj.putArray(nextResourceElement.name());
            } else {
                oj.putObject(nextResourceElement.name());
            }
        }
        JsonNode nextJsonNode = jn.get(nextResourceElement.name());
        return navigateAndCreateJson(nextResourceElement, nextJsonNode, f);
    }

}
