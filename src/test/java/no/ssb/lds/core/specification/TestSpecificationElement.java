package no.ssb.lds.core.specification;

import no.ssb.lds.api.json.JsonNavigationPath;
import no.ssb.lds.api.specification.SpecificationElement;
import no.ssb.lds.api.specification.SpecificationElementType;
import no.ssb.lds.api.specification.SpecificationValidator;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class TestSpecificationElement implements SpecificationElement {

    private final String name;
    private SpecificationElement parent;
    private final SpecificationElementType specificationElementType;
    private final Set<String> jsonTypes;
    private final List<SpecificationValidator> validators;
    private final Set<String> refTypes;
    private final Map<String, SpecificationElement> properties;
    private final SpecificationElement items;
    private boolean required = false;

    public TestSpecificationElement(String name, SpecificationElementType specificationElementType, Set<String> jsonTypes, List<SpecificationValidator> validators, Set<String> refTypes, Map<String, SpecificationElement> properties, SpecificationElement items) {
        this.name = name;
        this.specificationElementType = specificationElementType;
        this.jsonTypes = jsonTypes;
        this.validators = validators;
        this.refTypes = refTypes;
        this.properties = properties;
        this.items = items;
    }

    public TestSpecificationElement parent(SpecificationElement parent) {
        this.parent = parent;
        return this;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getDescription() {
        return "Description: " + toString();
    }

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

    void setRequired(boolean required) {
        this.required = required;
    }

    @Override
    public boolean isRequired() {
        return required;
    }

    @Override
    public Set<String> getRequired() {
        return getProperties().keySet().stream()
                .filter(propertyName -> getProperties().get(propertyName).isRequired())
                .collect(Collectors.toSet());
    }

    @Override
    public String toString() {
        return "TestSpecificationElement{" +
                "path='" + JsonNavigationPath.from(this).serialize() + '\'' +
                ", Type=" + specificationElementType +
                ", jsonTypes=" + jsonTypes +
                '}';
    }
}
