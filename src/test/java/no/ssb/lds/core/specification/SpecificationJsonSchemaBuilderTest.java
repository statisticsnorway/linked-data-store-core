package no.ssb.lds.core.specification;

import no.ssb.lds.api.json.JsonNavigationPath;
import no.ssb.lds.api.specification.Specification;
import no.ssb.lds.api.specification.SpecificationElementType;
import no.ssb.lds.api.specification.SpecificationTraversal;
import no.ssb.lds.core.schema.JsonSchema;
import no.ssb.lds.core.schema.JsonSchema04Builder;
import no.ssb.lds.core.utils.FileAndClasspathReaderUtils;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;

import static org.testng.Assert.assertEquals;

public class SpecificationJsonSchemaBuilderTest {

    @Test
    public void thatSpecificationBuiltFromJsonSchemaWithCustomLinkingWorks() {
        String contactJson = FileAndClasspathReaderUtils.getResourceAsString("spec/schemas/contact.json", StandardCharsets.UTF_8);
        String paJson = FileAndClasspathReaderUtils.getResourceAsString("spec/schemas/provisionagreement.json", StandardCharsets.UTF_8);
        JsonSchema jsonSchema = new JsonSchema04Builder(
                null, "contact", contactJson).build();
        new JsonSchema04Builder(
                jsonSchema, "contact", paJson).build();
        Specification specification = SpecificationJsonSchemaBuilder.createBuilder(null, null, jsonSchema).build();
        print(specification);
    }

    private void print(Specification specification) {
        SpecificationTraversal.depthFirstPreOrderFullTraversal(specification.getRootElement(), (ancestors, te) -> {
            if (te.getRefTypes() == null) {
                System.out.printf("%s%-10s %s  %s %s\n",
                        spaces(ancestors.size()),
                        te.getSpecificationElementType().name(),
                        te.getName(),
                        te.getJsonTypes(), te.isRequired() ? "required" : ""
                );
            } else {
                System.out.printf("%s%-10s %s %s %s ref: %s\n",
                        spaces(ancestors.size()),
                        te.getSpecificationElementType().name(),
                        te.getName(),
                        te.getJsonTypes(),
                        te.isRequired() ? "required" : "",
                        te.getRefTypes()
                );
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

    @Test
    public void thatReferencedTypesDoNotCreateMoreNestedRootOrManagedElementTypes() {
        Specification specification = JsonSchemaBasedSpecification.create(
                "spec/agent/Agent.json"
        );
        assertEquals(JsonNavigationPath.from("$").toSpecificationElement(specification, "Agent").getSpecificationElementType(), SpecificationElementType.MANAGED);
        AtomicInteger rootCounter = new AtomicInteger();
        AtomicInteger managedCounter = new AtomicInteger();
        AtomicInteger embeddedCounter = new AtomicInteger();
        AtomicInteger referenceCounter = new AtomicInteger();
        SpecificationTraversal.depthFirstPreOrderFullTraversal(specification.getRootElement(), (ancestors, element) -> {
            switch (element.getSpecificationElementType()) {
                case ROOT:
                    rootCounter.incrementAndGet();
                    break;
                case MANAGED:
                    managedCounter.incrementAndGet();
                    break;
                case EMBEDDED:
                    embeddedCounter.incrementAndGet();
                    break;
                case REF:
                    referenceCounter.incrementAndGet();
                    break;
            }
        });
        assertEquals(rootCounter.get(), 1);
        assertEquals(managedCounter.get(), 1);
        assertEquals(embeddedCounter.get(), 26);
        assertEquals(referenceCounter.get(), 2);
        print(specification);
    }
}
