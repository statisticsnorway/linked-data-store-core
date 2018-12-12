package no.ssb.lds.core.linkeddata;

import no.ssb.lds.api.specification.Specification;
import no.ssb.lds.core.specification.JsonSchemaBasedSpecification;
import no.ssb.lds.core.utils.FileAndClasspathReaderUtils;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.stream.Collectors;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class LinkedDataTest {

    private static final Logger LOG = LoggerFactory.getLogger(LinkedDataTest.class);

    @Test
    public void thatUnderscoreDelimitedUpperCaseAllowsFreakyCharacter() {
        assertEquals("A_VALUE_DO_MAIN_OBJECT", LinkedData.toUnderscoreDelimitedUpperCase("AValue#Do/mainObject"));
    }

    @Test
    public void thatJsonDocumentTraversal() {
        String siriusJson = FileAndClasspathReaderUtils.getResourceAsString("spec/schemas.examples/provisionagreement_sirius.json", StandardCharsets.UTF_8);
        JSONObject sirius = new JSONObject(siriusJson);
        Specification specification = JsonSchemaBasedSpecification.create(
                "spec/schemas/contact.json",
                "spec/schemas/provisionagreement.json"
        );
        JsonDocumentTraversal.walk(specification, "provisionagreement", sirius, (ancestors, jte) -> {
            if (jte.isReference()) {
                if (jte.isValueOrArrayValue())
                    assertEquals(jte.value.toString().split("/").length, 3); // valid managed-uri

                if (jte.isValue()) {
                    String[] uri = jte.uri(ancestors);
                    assertFalse(uri[uri.length - 1].endsWith(jte.value.toString())); // value node should not have managed-uri in node-path
                }

                if (jte.isArrayValue()) {
                    String[] uri = jte.uri(ancestors);
                    assertTrue((uri[uri.length - 1]).endsWith(JsonTraversalElement.ensureNoPrefixSlash(jte.value.toString()))); // value node should have managed-uri in node-path
                }

                //LOG.trace("[{}]{} {}: {} {} {}", ancestors.size(), indent(ancestors.size()), jte.key, jte.value, jte.type.typeName, jte.uri(ancestors));
            }
        });
    }

    @Test
    public void thatWePrintLinkedData() {
        String sirius = FileAndClasspathReaderUtils.getResourceAsString("spec/schemas.examples/provisionagreement_sirius.json", StandardCharsets.UTF_8);
        Specification specification = JsonSchemaBasedSpecification.create(
                "spec/schemas/contact.json",
                "spec/schemas/provisionagreement.json"
        );
        Collection<OutgoingLink> outgoingLinks = new LinkedData(specification, "data", "provisionagreement", "200", new JSONObject(sirius)).parse();
        assertEquals(outgoingLinks.stream().map(r -> r.relationshipName).collect(Collectors.toSet()).size(), 5);
        assertEquals(outgoingLinks.size(), 8);

        StringBuilder sb = new StringBuilder();
        sb.append("OutgoingLinks:\n");
        outgoingLinks.forEach(resource -> sb.append("  ")
                .append(resource.resourceURL)
                .append(" :").append(resource.relationshipName)
                .append(" -> ").append(resource.edgeResourceURL)
                .append(" => ").append(resource.relationshipURI)
                .append(" [").append(resource.edgeEntity)
                .append(":").append(resource.edgeId).append("]").append("\n"));

        LOG.debug("\n{}", sb.toString());
    }

}
