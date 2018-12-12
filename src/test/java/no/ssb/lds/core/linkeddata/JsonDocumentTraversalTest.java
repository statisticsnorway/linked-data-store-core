package no.ssb.lds.core.linkeddata;

import no.ssb.lds.api.specification.Specification;
import no.ssb.lds.core.specification.JsonSchemaBasedSpecification;
import no.ssb.lds.core.utils.FileAndClasspathReaderUtils;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;

public class JsonDocumentTraversalTest {

    private static final Logger LOG = LoggerFactory.getLogger(JsonDocumentTraversalTest.class);

    final String JSON = FileAndClasspathReaderUtils.getResourceAsString("spec/schemas.examples/provisionagreement_sirius.json", StandardCharsets.UTF_8);

    @Test
    public void thatArrayLinksAreWorking() {
        JSONObject doc = new JSONObject(JSON);
        Specification specification = JsonSchemaBasedSpecification.create(
                "spec/schemas/contact.json",
                "spec/schemas/provisionagreement.json"
        );
        AtomicInteger n = new AtomicInteger(0);
        JsonDocumentTraversal.walk(specification, "provisionagreement", doc, (ancestor, jte) -> {
            if (jte.isReference()) {
                LOG.info("{} -> {} -> {}", jte.type, jte.isArrayValue() ? jte.getSpecificationElement().getName() + "[" + jte.key + "]" : jte.key, jte.value);
                n.incrementAndGet();
            }
        });
        Assert.assertEquals(n.get(), 8);
    }
}
