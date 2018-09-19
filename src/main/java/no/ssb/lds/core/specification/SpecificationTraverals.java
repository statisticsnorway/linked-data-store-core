package no.ssb.lds.core.specification;

import java.util.Deque;
import java.util.LinkedList;
import java.util.Map;
import java.util.function.BiConsumer;

public class SpecificationTraverals {

    public static void depthFirstPreOrderFullTraversal(SpecificationElement element, BiConsumer<Deque<SpecificationElement>, SpecificationElement> visit) {
        Deque<SpecificationElement> ancestors = new LinkedList<>();
        visit.accept(ancestors, element);
        ancestors.addLast(element);
        doDepthFirstPreOrderFullTraversal(element, ancestors, visit);
    }

    static void doDepthFirstPreOrderFullTraversal(SpecificationElement element, Deque<SpecificationElement> ancestors, BiConsumer<Deque<SpecificationElement>, SpecificationElement> visit) {
        if (element.getProperties() == null) {
            return;
        }
        for (Map.Entry<String, SpecificationElement> e : element.getProperties().entrySet()) {
            ImmutableSpecificationElement childElement = (ImmutableSpecificationElement) e.getValue();
            visit.accept(ancestors, childElement);
            ancestors.addLast(childElement);
            doDepthFirstPreOrderFullTraversal(childElement, ancestors, visit);
            ancestors.removeLast();
        }
    }
}
