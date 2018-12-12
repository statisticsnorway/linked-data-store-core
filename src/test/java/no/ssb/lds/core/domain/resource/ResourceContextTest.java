package no.ssb.lds.core.domain.resource;

import no.ssb.lds.api.specification.Specification;
import no.ssb.lds.core.specification.JsonSchemaBasedSpecification;
import org.testng.annotations.Test;

import java.time.ZoneId;
import java.time.ZonedDateTime;

public class ResourceContextTest {

    private Specification specification() {
        return JsonSchemaBasedSpecification.create(
                "spec/schemas/contact.json",
                "spec/schemas/provisionagreement.json"
        );
    }

    @Test
    public void thatResourcesWithoutManagedDomainDocumentIdAreValid() {
        ResourceContext.createResourceContext(specification(), "/ns/contact", ZonedDateTime.now(ZoneId.of("Etc/UTC")));
    }

    @Test(expectedExceptions = ResourceException.class)
    public void unmanaged() {
        ResourceContext.createResourceContext(specification(), "/ns/unmanaged", ZonedDateTime.now(ZoneId.of("Etc/UTC")));
    }

    @Test(expectedExceptions = ResourceException.class)
    public void emptyDocumentId() {
        ResourceContext.createResourceContext(specification(), "/ns/contact/%20", ZonedDateTime.now(ZoneId.of("Etc/UTC")));
    }

    @Test(expectedExceptions = ResourceException.class)
    public void invalidEmbeddedPath() {
        ResourceContext.createResourceContext(specification(), "/ns/contact/1/bad", ZonedDateTime.now(ZoneId.of("Etc/UTC")));
    }

    @Test(expectedExceptions = ResourceException.class)
    public void invalidReferenceWithMissingLinkTargetId() {
        ResourceContext.createResourceContext(specification(), "/ns/provisionagreement/1/friend/contact", ZonedDateTime.now(ZoneId.of("Etc/UTC")));
    }

    @Test(expectedExceptions = ResourceException.class)
    public void invalidNavigationThroughReference() {
        ResourceContext.createResourceContext(specification(), "/ns/provisionagreement/1/friend/contact/1/name", ZonedDateTime.now(ZoneId.of("Etc/UTC")));
    }
}
