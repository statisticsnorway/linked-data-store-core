package no.ssb.lds.core.linkeddata;

import no.ssb.lds.api.specification.SpecificationElement;
import no.ssb.lds.api.specification.SpecificationElementType;

import java.util.Deque;

class JsonTraversalElement {
    final String key;
    final Object value;
    final JsonElementType type;
    private SpecificationElement specificationElement;

    JsonTraversalElement(String key, Object value, JsonElementType type) {
        this.key = key;
        this.value = value;
        this.type = type;
    }

    static String ensureNoPrefixSlash(Object value) {
        String relativePath = String.valueOf(value);
        return relativePath.startsWith("/") ? relativePath.substring(1) : relativePath;
    }

    String[] uri(Deque<JsonTraversalElement> ancestors) {
        String[] path = new String[ancestors.size() + 1];
        int i = 0;
        for (JsonTraversalElement element : ancestors) {
            path[i++] = element.key;
        }
        path[i++] = JsonElementType.ARRAY_VALUE.equals(type) ? ensureNoPrefixSlash(value) : key;
        return path;
    }

    boolean hasSpecificationElement() {
        return specificationElement != null;
    }

    boolean isValue() {
        return JsonElementType.VALUE.equals(type);
    }

    boolean isArrayValue() {
        return JsonElementType.ARRAY_VALUE.equals(type);
    }

    boolean isValueOrArrayValue() {
        return isValue() || isArrayValue();
    }

    boolean isReference() {
        return hasSpecificationElement() && SpecificationElementType.REF.equals(specificationElement.getSpecificationElementType());
    }

    SpecificationElement getSpecificationElement() {
        return specificationElement;
    }

    void setSpecificationElement(SpecificationElement specificationElement) {
        this.specificationElement = specificationElement;
    }
}
