package no.ssb.lds.core.specification;

import java.util.Set;

public interface Specification {

    SpecificationElement getRootElement();

    Set<String> getManagedDomains();

    SpecificationElement getElement(String managedDomain, String[] path);
}
