package no.ssb.lds.core.specification;

import no.ssb.lds.api.json.JsonNavigationPath;
import no.ssb.lds.api.specification.Specification;
import no.ssb.lds.api.specification.SpecificationElementType;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class JsonSchemaBasedSpecificationTest {

    @Test
    public void thatRefLeafNodesHaveCorrectType() {
        Specification specification = JsonSchemaBasedSpecification.create(
                "spec/schemas/contact.json",
                "spec/schemas/provisionagreement.json"
        );
        assertEquals(JsonNavigationPath.from("$", "friend").toSpecificationElement(specification, "provisionagreement").getSpecificationElementType(), SpecificationElementType.REF);
        assertEquals(JsonNavigationPath.from("$", "contacts").toSpecificationElement(specification, "provisionagreement").getSpecificationElementType(), SpecificationElementType.REF);
        assertEquals(JsonNavigationPath.from("$", "support", "technicalSupport").toSpecificationElement(specification, "provisionagreement").getSpecificationElementType(), SpecificationElementType.REF);
        assertEquals(JsonNavigationPath.from("$", "support", "businessSupport").toSpecificationElement(specification, "provisionagreement").getSpecificationElementType(), SpecificationElementType.REF);
        assertEquals(JsonNavigationPath.from("$", "support", "otherSupport").toSpecificationElement(specification, "provisionagreement").getSpecificationElementType(), SpecificationElementType.REF);

        assertEquals(JsonNavigationPath.from("$", "name").toSpecificationElement(specification, "contact").getSpecificationElementType(), SpecificationElementType.EMBEDDED);
        assertEquals(JsonNavigationPath.from("$", "age").toSpecificationElement(specification, "contact").getSpecificationElementType(), SpecificationElementType.EMBEDDED);
        assertEquals(JsonNavigationPath.from("$", "email").toSpecificationElement(specification, "contact").getSpecificationElementType(), SpecificationElementType.EMBEDDED);
    }
}
