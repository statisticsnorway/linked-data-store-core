package no.ssb.lds.core.domain.resource;

import no.ssb.lds.core.specification.JsonSchemaBasedSpecification;
import no.ssb.lds.core.specification.Specification;
import org.testng.annotations.Test;

public class ResourceContextTest {

    private Specification specification() {
        return JsonSchemaBasedSpecification.create(
                "spec/schemas/contact.json",
                "spec/schemas/provisionagreement.json"
        );
    }

    @Test
    public void thatResourcesWithoutManagedDomainDocumentIdAreValid() {
        ResourceContext.createResourceContext(specification(), "/ns/contact");
    }

    @Test(expectedExceptions = ResourceException.class)
    public void unmanaged() {
        ResourceContext.createResourceContext(specification(), "/ns/unmanaged");
    }

    @Test(expectedExceptions = ResourceException.class)
    public void emptyDocumentId() {
        ResourceContext.createResourceContext(specification(), "/ns/contact/%20");
    }

    @Test(expectedExceptions = ResourceException.class)
    public void invalidEmbeddedPath() {
        ResourceContext.createResourceContext(specification(), "/ns/contact/1/bad");
    }

    @Test(expectedExceptions = ResourceException.class)
    public void invalidReferenceWithMissingLinkTargetId() {
        ResourceContext.createResourceContext(specification(), "/ns/provisionagreement/1/friend/contact");
    }

    @Test(expectedExceptions = ResourceException.class)
    public void invalidNavigationThroughReference() {
        ResourceContext.createResourceContext(specification(), "/ns/provisionagreement/1/friend/contact/1/name");
    }
}
