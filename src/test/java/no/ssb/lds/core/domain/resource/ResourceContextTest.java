package no.ssb.lds.core.domain.resource;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import no.ssb.lds.api.json.JsonNavigationPath;
import no.ssb.lds.api.persistence.json.JsonDocument;
import no.ssb.lds.api.specification.Specification;
import no.ssb.lds.api.specification.SpecificationElementType;
import no.ssb.lds.core.specification.SpecificationBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Set;

import static no.ssb.lds.core.specification.SpecificationBuilder.arrayRefNode;
import static no.ssb.lds.core.specification.SpecificationBuilder.objectNode;
import static no.ssb.lds.core.specification.SpecificationBuilder.refNode;
import static no.ssb.lds.core.specification.SpecificationBuilder.stringNode;

public class ResourceContextTest {

    public static Specification specification() {
        return SpecificationBuilder.createSpecificationAndRoot(Set.of(
                objectNode(SpecificationElementType.MANAGED, "SomeEntity", Set.of(
                        stringNode("aString"),
                        refNode("ref", Set.of("OtherEntity")),
                        arrayRefNode("refs", Set.of("OtherEntity")),
                        objectNode("object1", Set.of(
                                stringNode("aString"),
                                refNode("ref", Set.of("OtherEntity")),
                                arrayRefNode("refs", Set.of("OtherEntity")),
                                objectNode("object2", Set.of(
                                        stringNode("aString"),
                                        refNode("ref", Set.of("OtherEntity")),
                                        arrayRefNode("refs", Set.of("OtherEntity"))
                                ))
                        ))
                )),
                objectNode(SpecificationElementType.MANAGED, "OtherEntity", Set.of(
                        stringNode("aString")
                ))
        ));
    }

    @Test
    public void thatCreateResourceContextOnManagedResourceWorks() {
        Specification specification = specification();
        ResourceContext context = ResourceContext.createResourceContext(specification, "/ns/SomeEntity/123", ZonedDateTime.now(ZoneId.of("Etc/UTC")));
        Assert.assertTrue(context.isManaged());
    }

    @Test
    public void thatCreateResourceContextOnEmbeddedResourceWorks() {
        Specification specification = specification();
        ResourceContext context = ResourceContext.createResourceContext(specification, "/ns/SomeEntity/123/object1", ZonedDateTime.now(ZoneId.of("Etc/UTC")));
        Assert.assertTrue(context.isEmbedded());
    }

    @Test
    public void thatCreateResourceContextOnEmbeddedLinkResourceWorks() {
        Specification specification = specification();
        ResourceContext context = ResourceContext.createResourceContext(specification, "/ns/SomeEntity/123/ref", ZonedDateTime.now(ZoneId.of("Etc/UTC")));
        Assert.assertTrue(context.isEmbedded());
    }

    @Test
    public void thatCreateResourceContextOnNestedEmbeddedResourceWorks() {
        Specification specification = specification();
        ResourceContext context = ResourceContext.createResourceContext(specification, "/ns/SomeEntity/123/object1/object2", ZonedDateTime.now(ZoneId.of("Etc/UTC")));
        Assert.assertTrue(context.isEmbedded());
    }

    @Test
    public void thatCreateResourceContextOnReferenceResourceWorks() {
        Specification specification = specification();
        ResourceContext context = ResourceContext.createResourceContext(specification, "/ns/SomeEntity/123/ref/OtherEntity/456", ZonedDateTime.now(ZoneId.of("Etc/UTC")));
        Assert.assertTrue(context.isReference());
    }

    @Test
    public void thatCreateResourceContextOnNestedReferenceResourceWorks() {
        Specification specification = specification();
        ResourceContext context = ResourceContext.createResourceContext(specification, "/ns/SomeEntity/123/object1/ref/OtherEntity/456", ZonedDateTime.now(ZoneId.of("Etc/UTC")));
        Assert.assertTrue(context.isReference());
    }

    @Test
    public void thatCreateResourceContextOnNestedNestedReferenceResourceWorks() {
        Specification specification = specification();
        ResourceContext context = ResourceContext.createResourceContext(specification, "/ns/SomeEntity/123/object1/object2/ref/OtherEntity/456", ZonedDateTime.now(ZoneId.of("Etc/UTC")));
        Assert.assertTrue(context.isReference());
    }

    @Test
    public void thatCreateResourceContextOnArrayReferenceResourceWorks() {
        Specification specification = specification();
        ResourceContext context = ResourceContext.createResourceContext(specification, "/ns/SomeEntity/123/refs/OtherEntity/456", ZonedDateTime.now(ZoneId.of("Etc/UTC")));
        Assert.assertTrue(context.isReference());
    }

    @Test
    public void thatCreateResourceContextOnNestedArrayReferenceResourceWorks() {
        Specification specification = specification();
        ResourceContext context = ResourceContext.createResourceContext(specification, "/ns/SomeEntity/123/object1/refs/OtherEntity/456", ZonedDateTime.now(ZoneId.of("Etc/UTC")));
        Assert.assertTrue(context.isReference());
    }

    @Test
    public void thatCreateResourceContextOnNestedNestedArrayReferenceResourceWorks() {
        Specification specification = specification();
        ResourceContext context = ResourceContext.createResourceContext(specification, "/ns/SomeEntity/123/object1/object2/refs/OtherEntity/456", ZonedDateTime.now(ZoneId.of("Etc/UTC")));
        Assert.assertTrue(context.isReference());
    }

    @Test
    public void thatNavigateOnNestedNestedArrayReferenceResourceWorksOnEmptyDocument() throws JsonProcessingException {
        Specification specification = specification();
        ResourceContext context = ResourceContext.createResourceContext(specification, "/ns/SomeEntity/123/object1/object2/refs/OtherEntity/456", ZonedDateTime.now(ZoneId.of("Etc/UTC")));
        JsonNode documentRoot = JsonDocument.mapper.createObjectNode();
        context.navigateAndCreateJson(documentRoot, t -> {
            Assert.assertEquals(t.resourceElement.getSpecificationElement().getSpecificationElementType(), SpecificationElementType.REF);
            Assert.assertEquals(JsonNavigationPath.from(t.resourceElement.getSpecificationElement()).serialize(), "$.object1.object2.refs");
            try {
                Assert.assertEquals(JsonDocument.mapper.writeValueAsString(t.jsonObject), "{}");
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
            return null;
        });
        Assert.assertEquals(JsonDocument.mapper.writeValueAsString(documentRoot), "{\"object1\":{\"object2\":{}}}");
    }

    @Test
    public void thatNavigateOnNestedNestedArrayReferenceResourceWorksOnPopulatedDocument() throws JsonProcessingException {
        Specification specification = specification();
        ResourceContext context = ResourceContext.createResourceContext(specification, "/ns/SomeEntity/123/object1/object2/refs/OtherEntity/456", ZonedDateTime.now(ZoneId.of("Etc/UTC")));
        ObjectNode documentRoot = JsonDocument.mapper.createObjectNode();
        documentRoot.putObject("object1").putObject("object2").putArray("refs").add("/OtherEntity/1");
        context.navigateAndCreateJson(documentRoot, t -> {
            Assert.assertEquals(t.resourceElement.getSpecificationElement().getSpecificationElementType(), SpecificationElementType.REF);
            Assert.assertEquals(JsonNavigationPath.from(t.resourceElement.getSpecificationElement()).serialize(), "$.object1.object2.refs");
            try {
                Assert.assertEquals(JsonDocument.mapper.writeValueAsString(t.jsonObject), "{\"refs\":[\"/OtherEntity/1\"]}");
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
            return null;
        });
        Assert.assertEquals(JsonDocument.mapper.writeValueAsString(documentRoot), "{\"object1\":{\"object2\":{\"refs\":[\"/OtherEntity/1\"]}}}");
    }

    @Test
    public void thatNavigateOnNestedNestedArrayReferenceResourceWorksOnPartlyPopulatedDocument() throws JsonProcessingException {
        Specification specification = specification();
        ResourceContext context = ResourceContext.createResourceContext(specification, "/ns/SomeEntity/123/object1/object2/refs/OtherEntity/456", ZonedDateTime.now(ZoneId.of("Etc/UTC")));
        ObjectNode documentRoot = JsonDocument.mapper.createObjectNode();
        documentRoot.putObject("object1");
        context.navigateAndCreateJson(documentRoot, t -> {
            Assert.assertEquals(t.resourceElement.getSpecificationElement().getSpecificationElementType(), SpecificationElementType.REF);
            Assert.assertEquals(JsonNavigationPath.from(t.resourceElement.getSpecificationElement()).serialize(), "$.object1.object2.refs");
            try {
                Assert.assertEquals(JsonDocument.mapper.writeValueAsString(t.jsonObject), "{}");
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
            return null;
        });
        Assert.assertEquals(JsonDocument.mapper.writeValueAsString(documentRoot), "{\"object1\":{\"object2\":{}}}");
    }

    // TODO Test array-navigation
}
