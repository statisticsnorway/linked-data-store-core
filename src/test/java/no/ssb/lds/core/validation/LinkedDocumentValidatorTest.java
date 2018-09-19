package no.ssb.lds.core.validation;

import no.ssb.lds.core.specification.JsonSchemaBasedSpecification;
import org.json.JSONObject;
import org.testng.annotations.Test;

public class LinkedDocumentValidatorTest {

    @Test
    public void thatValidLinkedDocumentPassesSchemaValidation() {
        JsonSchemaBasedSpecification specification = JsonSchemaBasedSpecification.create("spec/schemas/contact.json", "spec/schemas/provisionagreement.json");
        LinkedDocumentValidator validator = new LinkedDocumentValidator(specification, specification);

        String linkedDocument = "{\"id\":\"r13\",\"name\":\"pa-test-name\",\"friend\":\"/contact/f1\",\"support\":{\"technicalSupport\":[\"/contact/s1\",\"/contact/s2\"],\"businessSupport\":[\"/contact/b1\"]}}";
        validator.validate("provisionagreement", new JSONObject(linkedDocument));
    }

    @Test(expectedExceptions = LinkedDocumentValidationException.class)
    public void thatSingleLinkToInvalidDomainFailsSchemaValidation() {
        JsonSchemaBasedSpecification specification = JsonSchemaBasedSpecification.create("spec/schemas/contact.json", "spec/schemas/provisionagreement.json");
        LinkedDocumentValidator validator = new LinkedDocumentValidator(specification, specification);

        String linkedDocument = "{\"id\":\"r13\",\"name\":\"pa-test-name\",\"friend\":\"/baddomain/f1\",\"support\":{\"technicalSupport\":[\"/contact/s1\",\"/contact/s2\"],\"businessSupport\":[\"/contact/b1\"]}}";
        validator.validate("provisionagreement", new JSONObject(linkedDocument));
    }

    @Test(expectedExceptions = LinkedDocumentValidationException.class)
    public void thatArrayLinkToInvalidDomainFailsSchemaValidation() {
        JsonSchemaBasedSpecification specification = JsonSchemaBasedSpecification.create("spec/schemas/contact.json", "spec/schemas/provisionagreement.json");
        LinkedDocumentValidator validator = new LinkedDocumentValidator(specification, specification);

        String linkedDocument = "{\"id\":\"r13\",\"name\":\"pa-test-name\",\"friend\":\"/contact/f1\",\"support\":{\"technicalSupport\":[\"/contact/s1\",\"/baddomain/s2\"],\"businessSupport\":[\"/contact/b1\"]}}";
        validator.validate("provisionagreement", new JSONObject(linkedDocument));
    }

    @Test(expectedExceptions = LinkedDocumentValidationException.class)
    public void thatSingleMalformedLinkFailsSchemaValidation() {
        JsonSchemaBasedSpecification specification = JsonSchemaBasedSpecification.create("spec/schemas/contact.json", "spec/schemas/provisionagreement.json");
        LinkedDocumentValidator validator = new LinkedDocumentValidator(specification, specification);

        String linkedDocument = "{\"id\":\"r13\",\"name\":\"pa-test-name\",\"friend\":\"contact\",\"support\":{\"technicalSupport\":[\"/contact/s1\",\"/contact/s2\"],\"businessSupport\":[\"/contact/b1\"]}}";
        validator.validate("provisionagreement", new JSONObject(linkedDocument));
    }

    @Test(expectedExceptions = LinkedDocumentValidationException.class)
    public void thatArrayMalformedLinkFailsSchemaValidation() {
        JsonSchemaBasedSpecification specification = JsonSchemaBasedSpecification.create("spec/schemas/contact.json", "spec/schemas/provisionagreement.json");
        LinkedDocumentValidator validator = new LinkedDocumentValidator(specification, specification);

        String linkedDocument = "{\"id\":\"r13\",\"name\":\"pa-test-name\",\"friend\":\"/contact/f1\",\"support\":{\"technicalSupport\":[\"/contact/s1\",\"contact\"],\"businessSupport\":[\"/contact/b1\"]}}";
        validator.validate("provisionagreement", new JSONObject(linkedDocument));
    }
}
