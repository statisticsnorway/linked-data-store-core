package no.ssb.lds.core.specification;

import no.ssb.lds.api.specification.SpecificationElement;
import no.ssb.lds.api.specification.SpecificationElementType;
import no.ssb.lds.api.specification.SpecificationValidator;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Optional.ofNullable;

/**
 * Immutable and thread-safe after creation.
 */
class ImmutableSpecificationElement implements SpecificationElement {

    private final String name;
    private final String description;
    private final SpecificationElement parent;
    private final SpecificationElementType specificationElementType;
    private final Set<String> jsonTypes;
    private final List<SpecificationValidator> validators;
    private final Set<String> refTypes;
    private final Map<String, SpecificationElement> properties;
    private final SpecificationElement items;

    ImmutableSpecificationElement(SpecificationElementBuilder elementBuilder) {
        elementBuilder.specificationElement(this);
        this.name = elementBuilder.name();
        this.description = elementBuilder.description();
        this.parent = elementBuilder.parent();
        this.specificationElementType = elementBuilder.specificationElementType();
        this.jsonTypes = ofNullable(elementBuilder.jsonTypes()).map(Collections::unmodifiableSet).orElse(Collections.emptySet());
        this.validators = ofNullable(elementBuilder.validators()).map(Collections::unmodifiableList).orElse(Collections.emptyList());
        this.refTypes = ofNullable(elementBuilder.refTypes()).map(Collections::unmodifiableSet).orElse(Collections.emptySet());
        this.properties = ofNullable(elementBuilder.properties()).map(Collections::unmodifiableMap).orElse(Collections.emptyMap());
        this.items = elementBuilder.items();
    }

    @Override
    public String getName() {
        return name;
    }

    //TODO @Override
    //public String getDescription() {
    //    return description;
    //}

    @Override
    public SpecificationElement getParent() {
        return parent;
    }

    @Override
    public SpecificationElementType getSpecificationElementType() {
        return specificationElementType;
    }

    @Override
    public Set<String> getJsonTypes() {
        return jsonTypes;
    }

    @Override
    public List<SpecificationValidator> getValidators() {
        return validators;
    }

    @Override
    public Set<String> getRefTypes() {
        return refTypes;
    }

    @Override
    public Map<String, SpecificationElement> getProperties() {
        return properties;
    }

    @Override
    public SpecificationElement getItems() {
        return items;
    }
}
