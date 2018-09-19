package no.ssb.lds.core.specification;

import no.ssb.lds.core.schema.JsonSchema;
import no.ssb.lds.core.schema.JsonSchema04Builder;
import no.ssb.lds.core.utils.FileAndClasspathReaderUtils;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;

public class SpecificationJsonSchemaBuilderTest {

    @Test
    public void thatSpecificationBuiltFromJsonSchemaWithCustomLinkingWorks() {
        String contactJson = FileAndClasspathReaderUtils.getResourceAsString("spec/schemas/contact.json", StandardCharsets.UTF_8);
        String paJson = FileAndClasspathReaderUtils.getResourceAsString("spec/schemas/provisionagreement.json", StandardCharsets.UTF_8);
        JsonSchema jsonSchema = new JsonSchema04Builder(
                null, "contact", contactJson).build();
        new JsonSchema04Builder(
                jsonSchema, "contact", paJson).build();
        Specification specification = SpecificationJsonSchemaBuilder.createBuilder(jsonSchema).build();
        SpecificationTraverals.depthFirstPreOrderFullTraversal(specification.getRootElement(), (ancestors, te) -> {
            if (te.getRefTypes() == null) {
                System.out.printf("%s%-10s %s  %s\n", spaces(ancestors.size()), te.getSpecificationElementType().name(), te.getName(), te.getJsonTypes());
            } else {
                System.out.printf("%s%-10s %s %s ref: %s\n", spaces(ancestors.size()), te.getSpecificationElementType().name(), te.getName(), te.getJsonTypes(), te.getRefTypes());
            }
        });
    }

    private String spaces(int n) {
        StringBuilder sb = new StringBuilder(n + 1);
        for (int i = 0; i < n; i++) {
            sb.append("  ");
        }
        return sb.toString();
    }
}
