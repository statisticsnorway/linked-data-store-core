package no.ssb.lds.core.validation;

import no.ssb.lds.api.specification.Specification;
import no.ssb.lds.api.specification.SpecificationElement;
import no.ssb.lds.api.specification.SpecificationElementType;
import no.ssb.lds.api.specification.SpecificationTraverals;
import no.ssb.lds.core.schema.SchemaRepository;
import org.everit.json.schema.Schema;
import org.everit.json.schema.ValidationException;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Deque;
import java.util.LinkedList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class LinkedDocumentValidator {

    private static final Logger LOG = LoggerFactory.getLogger(LinkedDocumentValidator.class);

    private final Specification specification;
    private final SchemaRepository schemaRepository;
    private final Pattern linkPattern = Pattern.compile("/([^/]+)/([^/]+)");

    public LinkedDocumentValidator(Specification specification, SchemaRepository schemaRepository) {
        this.specification = specification;
        this.schemaRepository = schemaRepository;
    }

    public void validate(String managedDomain, String document) throws LinkedDocumentValidationException {

        // TODO Use already parsed Jackson instead of org.json. Requires a change of json-schema validation library
        JSONObject linkedDocument = new JSONObject(document);

        Schema schema = schemaRepository.getJsonSchema().getSchema(managedDomain);
        try {
            schema.validate(linkedDocument);
        } catch (ValidationException e) {
            e.getAllMessages().forEach(m -> LOG.debug("{}", m));
            throw new LinkedDocumentValidationException(e.getAllMessages().toString(), e);
        }
        SpecificationElement managedDomainElement = specification.getRootElement().getProperties().get(managedDomain);
        SpecificationTraverals.depthFirstPreOrderFullTraversal(managedDomainElement, (ancestors, te) -> {
            if (SpecificationElementType.REF != te.getSpecificationElementType()) {
                return;
            }
            JSONObject context = linkedDocument;
            for (SpecificationElement ancestor : ancestors) {
                if (SpecificationElementType.ROOT == ancestor.getSpecificationElementType()) {
                    continue;
                }
                if (SpecificationElementType.MANAGED == ancestor.getSpecificationElementType()) {
                    continue;
                }
                if (!context.has(ancestor.getName())) {
                    return;
                }
                context = context.getJSONObject(ancestor.getName());
            }
            if (!context.has(te.getName())) {
                return;
            }
            if (te.getJsonTypes().contains("array")) {
                JSONArray linkArray = context.getJSONArray(te.getName());
                for (int i = 0; i < linkArray.length(); i++) {
                    String link = linkArray.getString(i);
                    validateLink(ancestors, te, link);
                }
            } else {
                String link = context.optString(te.getName(), null);
                if (link != null) {
                    validateLink(ancestors, te, link);
                }
            }
        });
    }

    private void validateLink(Deque<SpecificationElement> ancestors, SpecificationElement te, String link) throws LinkedDocumentValidationException {
        Matcher m = linkPattern.matcher(link);
        if (!m.matches()) {
            throw new LinkedDocumentValidationException(String.format("Not a valid link. Navigation: %s: \"%s\"", printNavigationPath(ancestors, te), link));
        }
        String linkedDomain = m.group(1);
        if (!te.getRefTypes().contains(linkedDomain)) {
            throw new LinkedDocumentValidationException(String.format("Illegal linked-domain: \"%s\". Navigation: %s: \"%s\"", linkedDomain, printNavigationPath(ancestors, te), link));
        }
    }

    private String printNavigationPath(Deque<SpecificationElement> ancestors, SpecificationElement te) {
        Deque<SpecificationElement> debugListing = new LinkedList<>(ancestors);
        debugListing.removeFirst();
        debugListing.addLast(te);
        return debugListing.stream().map(e -> e.getName()).collect(Collectors.joining("."));
    }
}
