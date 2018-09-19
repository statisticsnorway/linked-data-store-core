package no.ssb.lds.core.specification;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class SpecificationTest {

    @Test
    public void thatRefLeafNodesHaveCorrectType() {
        Specification specification = JsonSchemaBasedSpecification.create(
                "spec/schemas/contact.json",
                "spec/schemas/provisionagreement.json"
        );
        assertEquals(specification.getElement("provisionagreement", new String[]{"friend"}).getSpecificationElementType(), SpecificationElementType.REF);
        assertEquals(specification.getElement("provisionagreement", new String[]{"contacts"}).getSpecificationElementType(), SpecificationElementType.REF);
        assertEquals(specification.getElement("provisionagreement", new String[]{"support", "technicalSupport"}).getSpecificationElementType(), SpecificationElementType.REF);
        assertEquals(specification.getElement("provisionagreement", new String[]{"support", "businessSupport"}).getSpecificationElementType(), SpecificationElementType.REF);
        assertEquals(specification.getElement("provisionagreement", new String[]{"support", "otherSupport"}).getSpecificationElementType(), SpecificationElementType.REF);

        assertEquals(specification.getElement("contact", new String[]{"name"}).getSpecificationElementType(), SpecificationElementType.EMBEDDED);
        assertEquals(specification.getElement("contact", new String[]{"age"}).getSpecificationElementType(), SpecificationElementType.EMBEDDED);
        assertEquals(specification.getElement("contact", new String[]{"email"}).getSpecificationElementType(), SpecificationElementType.EMBEDDED);
    }
}
