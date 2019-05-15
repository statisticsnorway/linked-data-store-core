package no.ssb.lds.core.specification;

import no.ssb.lds.api.specification.SpecificationElement;
import no.ssb.lds.api.specification.SpecificationElementType;
import no.ssb.lds.api.specification.SpecificationValidator;
import no.ssb.lds.core.schema.JsonSchema;
import no.ssb.lds.core.schema.JsonSchemaDefinitionElement;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

class SpecificationElementBuilder {

    private final JsonSchemaDefinitionElement schemaElement;

    private JsonSchema jsonSchema;
    private SpecificationElement specificationElement;
    private String name;
    private String description;
    private SpecificationElement parent;
    private SpecificationElementType specificationElementType;
    private Set<String> refTypes;
    private Set<String> jsonTypes;
    private Set<String> required = Collections.emptySet();

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

    String description() {
        if (this.schemaElement != null) {
            return this.schemaElement.description;
        } else {
            return description;
        }
    }

    SpecificationElementBuilder description(String description) {
        this.description = description;
        return this;
    }

    SpecificationElement parent() {
        return parent;
    }

    SpecificationElementBuilder parent(SpecificationElement parent) {
        this.parent = parent;
        return this;
    }

    SpecificationElementBuilder required(Set<String> required) {
        this.required = Objects.requireNonNull(required);
        return this;
    }

    SpecificationElementBuilder specificationElementType(SpecificationElementType specificationElementType) {
        this.specificationElementType = specificationElementType;
        return this;
    }

    SpecificationElementType specificationElementType() {
        return specificationElementType;
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
        // Name is required for graphql implementation.
        // See comment in JsonSchemaDefinitionElement.java.
        String name = schemaElement.items.name != null ? schemaElement.items.name : "";
        SpecificationElement child = new SpecificationElementBuilder(schemaElement.items)
                // TODO(kim): master uses .name("[]"). I (hadrien) need it to link types in graphql.
                .name(name)
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
                SpecificationElementType specificationElementType;
                if (jsonSchema.getSchemaNames().contains(entry.getKey())) {
                    specificationElementType = SpecificationElementType.MANAGED;
                } else {
                    specificationElementType = SpecificationElementType.EMBEDDED;
                }
                SpecificationElement managed = new SpecificationElementBuilder(entry.getValue())
                        .name(entry.getKey())
                        .parent(specificationElement)
                        .specificationElementType(specificationElementType)
                        .required(entry.getValue().required)
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
                    .description(e.getValue().description)
                    .parent(specificationElement)
                    .specificationElementType(SpecificationElementType.EMBEDDED)
                    .refTypes(refTypes);
            if (refTypes != null) {
                childBuilder.specificationElementType(SpecificationElementType.REF);
            }
            SpecificationElement child = childBuilder.build();
            map.put(e.getKey(), child);
        }
        return map;
    }

    public Set<String> required() {
        return required != null ? required : Collections.emptySet();
    }
}
