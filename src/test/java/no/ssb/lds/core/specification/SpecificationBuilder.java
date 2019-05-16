package no.ssb.lds.core.specification;

import no.ssb.lds.api.specification.Specification;
import no.ssb.lds.api.specification.SpecificationElement;
import no.ssb.lds.api.specification.SpecificationElementType;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class SpecificationBuilder {

    public static TestSpecificationElement booleanNode(String name) {
        return new TestSpecificationElement(name, SpecificationElementType.EMBEDDED, Set.of("boolean"), List.of(), Set.of(), Map.of(), null);
    }

    public static TestSpecificationElement numericNode(String name) {
        return new TestSpecificationElement(name, SpecificationElementType.EMBEDDED, Set.of("number"), List.of(), Set.of(), Map.of(), null);
    }

    public static TestSpecificationElement stringNode(String name) {
        return new TestSpecificationElement(name, SpecificationElementType.EMBEDDED, Set.of("string"), List.of(), Set.of(), Map.of(), null);
    }

    public static TestSpecificationElement arrayNode(String name, TestSpecificationElement items) {
        TestSpecificationElement arrayNode = new TestSpecificationElement(name, SpecificationElementType.EMBEDDED, Set.of("array"), List.of(), Set.of(), Map.of(), items);
        items.parent(arrayNode);
        return arrayNode;
    }

    public static TestSpecificationElement refNode(String name, Set<String> refTypes) {
        return new TestSpecificationElement(name, SpecificationElementType.REF, Set.of("string"), List.of(), refTypes, Map.of(), null);
    }

    public static TestSpecificationElement arrayRefNode(String name, Set<String> refTypes) {
        TestSpecificationElement arrayNode = new TestSpecificationElement(name, SpecificationElementType.REF, Set.of("array"), List.of(), refTypes, Map.of(), stringNode("[]"));
        return arrayNode;
    }

    public static TestSpecificationElement objectNode(String name, Set<TestSpecificationElement> properties) {
        return objectNode(SpecificationElementType.EMBEDDED, name, properties);
    }

    public static TestSpecificationElement required(TestSpecificationElement element) {
        if (element.getSpecificationElementType() == SpecificationElementType.ROOT) {
            throw new IllegalArgumentException("cannot set required on root object");
        }
        element.setRequired(true);
        return element;
    }

    public static TestSpecificationElement objectNode(SpecificationElementType elementType, String name, Set<TestSpecificationElement> properties) {
        TestSpecificationElement mapElement = new TestSpecificationElement(name, elementType, Set.of("object"), List.of(), Set.of(),
                new TreeMap<>(properties.stream().collect(Collectors.toMap(e -> e.getName(), e -> e))), null);
        properties.forEach(e -> e.parent(mapElement));
        return mapElement;
    }

    public static Specification createSpecificationAndRoot(Set<TestSpecificationElement> managedElements) {
        TestSpecificationElement root = new TestSpecificationElement(
                "root",
                SpecificationElementType.ROOT,
                Set.of("object"),
                List.of(),
                Set.of(),
                managedElements.stream().collect(Collectors.toMap(e -> e.getName(), e -> e)),
                null
        );
        managedElements.forEach(e -> e.parent(root));
        return new Specification() {
            @Override
            public SpecificationElement getRootElement() {
                return root;
            }

            @Override
            public Set<String> getManagedDomains() {
                return managedElements.stream().map(e -> e.getName()).collect(Collectors.toSet());
            }
        };
    }
}
