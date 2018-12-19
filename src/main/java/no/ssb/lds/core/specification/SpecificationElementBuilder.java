package no.ssb.lds.core.specification;

import no.ssb.lds.api.specification.SpecificationElement;
import no.ssb.lds.api.specification.SpecificationElementType;
import no.ssb.lds.api.specification.SpecificationValidator;
import no.ssb.lds.core.schema.JsonSchema;
import no.ssb.lds.core.schema.JsonSchemaDefinitionElement;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

class SpecificationElementBuilder {

    private final JsonSchemaDefinitionElement schemaElement;

    private JsonSchema jsonSchema;
    private SpecificationElement specificationElement;
    private String name;
    private SpecificationElement parent;
    private SpecificationElementType specificationElementType;
    private SpecificationElementType parentSpecificationElementType;
    private Set<String> refTypes;
    private Set<String> jsonTypes;

    SpecificationElementBuilder(JsonSchemaDefinitionElement schemaElement) {
        this.schemaElement = schemaElement;
    }

    SpecificationElement build() {
        return new ImmutableSpecificationElement(this);
    }

    SpecificationElementBuilder specificationElement(SpecificationElement specificationElement) {
        this.specificationElement = specificationElement;
        return this;
    }

    String name() {
        return name;
    }

    SpecificationElementBuilder name(String name) {
        this.name = name;
        return this;
    }

    SpecificationElement parent() {
        return parent;
    }

    SpecificationElementBuilder parent(SpecificationElement parent) {
        this.parent = parent;
        return this;
    }

    SpecificationElementBuilder specificationElementType(SpecificationElementType specificationElementType) {
        this.specificationElementType = specificationElementType;
        return this;
    }

    SpecificationElementType specificationElementType() {
        if (specificationElementType != null) {
            return specificationElementType;
        }
        if (parentSpecificationElementType == null) {
            return SpecificationElementType.ROOT;
        }
        if (SpecificationElementType.ROOT == parentSpecificationElementType) {
            return SpecificationElementType.MANAGED;
        }
        return SpecificationElementType.EMBEDDED;
    }

    SpecificationElementBuilder parentSpecificationElementType(SpecificationElementType parentSpecificationElementType) {
        this.parentSpecificationElementType = parentSpecificationElementType;
        return this;
    }

    SpecificationElementType parentSpecificationElementType() {
        return parentSpecificationElementType;
    }

    SpecificationElementBuilder jsonSchema(JsonSchema jsonSchema) {
        this.jsonSchema = jsonSchema;
        return this;
    }

    SpecificationElementBuilder jsonTypes(Set<String> jsonTypes) {
        this.jsonTypes = jsonTypes;
        return this;
    }

    Set<String> jsonTypes() {
        if (jsonTypes != null) {
            return jsonTypes;
        }
        Set<String> set = new LinkedHashSet<>();
        for (String type : schemaElement.types) {
            set.add(type);
        }
        return set;
    }

    SpecificationElementBuilder refTypes(Set<String> refTypes) {
        this.refTypes = refTypes;
        return this;
    }

    Set<String> refTypes() {
        return refTypes;
    }

    Set<String> linkedDomains(String property) {
        String linkPropertyName = "_link_property_" + property;
        if (!schemaElement.properties.containsKey(linkPropertyName)) {
            return null;
        }
        JsonSchemaDefinitionElement jsde = schemaElement.properties.get(linkPropertyName);
        Set<String> refTypes = new LinkedHashSet<>(jsde.properties.keySet());
        return refTypes;
    }

    List<SpecificationValidator> validators() {
        return null;
    }

    SpecificationElement items() {
        if (schemaElement == null || schemaElement.items == null) {
            return null;
        }
        SpecificationElement child = new SpecificationElementBuilder(schemaElement.items)
                .name("[]")
                .parent(specificationElement)
                .build();
        return child;
    }

    Map<String, SpecificationElement> properties() {
        Map<String, SpecificationElement> map = new LinkedHashMap<>();
        if (SpecificationElementType.ROOT.equals(specificationElementType)) {
            if (jsonSchema == null) {
                return map;
            }
            for (Map.Entry<String, JsonSchemaDefinitionElement> entry : jsonSchema.getDefinitions().entrySet()) {
                SpecificationElement managed = new SpecificationElementBuilder(entry.getValue())
                        .name(entry.getKey())
                        .parent(specificationElement)
                        .specificationElementType(SpecificationElementType.MANAGED)
                        .build();
                map.put(entry.getKey(), managed);
            }
            return map;
        }
        if (schemaElement == null || schemaElement.properties == null || schemaElement.properties.isEmpty()) {
            return null;
        }
        for (Map.Entry<String, JsonSchemaDefinitionElement> e : schemaElement.properties.entrySet()) {
            if (e.getKey().startsWith("_link_property_")) {
                continue;
            }
            Set<String> refTypes = linkedDomains(e.getKey());
            SpecificationElementBuilder childBuilder = new SpecificationElementBuilder(e.getValue())
                    .name(e.getKey())
                    .parent(specificationElement)
                    .parentSpecificationElementType(specificationElementType())
                    .refTypes(refTypes);
            if (refTypes != null) {
                childBuilder.specificationElementType(SpecificationElementType.REF);
            }
            SpecificationElement child = childBuilder.build();
            map.put(e.getKey(), child);
        }
        return map;
    }
}
