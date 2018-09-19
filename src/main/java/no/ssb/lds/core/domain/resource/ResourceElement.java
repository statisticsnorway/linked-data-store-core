package no.ssb.lds.core.domain.resource;

import no.ssb.lds.core.specification.SpecificationElement;

import java.util.Objects;

public class ResourceElement {

    private final SpecificationElement specificationElement;
    private final String name;
    private final String id;
    private final ResourceElement next;

    ResourceElement(SpecificationElement specificationElement, String name, String id, ResourceElement next) {
        this.specificationElement = specificationElement;
        this.name = name;
        this.id = id;
        this.next = next;
    }

    public SpecificationElement getSpecificationElement() {
        return specificationElement;
    }

    boolean hasNext() {
        return next != null;
    }

    ResourceElement next() {
        return next;
    }

    public String name() {
        return name;
    }

    public String id() {
        return id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ResourceElement that = (ResourceElement) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, id);
    }

    @Override
    public String toString() {
        return "ResourceElement{" +
                "name='" + name + '\'' +
                ", id='" + id + '\'' +
                '}';
    }
}
