package no.ssb.lds.core.linkeddata;

import no.ssb.lds.api.persistence.OutgoingLink;
import no.ssb.lds.core.specification.Specification;
import org.json.JSONObject;

import java.nio.CharBuffer;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Collectors;

public class LinkedData {

    private final Specification specification;
    private final String namespace;
    private final String entity;
    private final String id;
    private final JSONObject doc;

    public LinkedData(Specification specification, String namespace, String entity, String id, JSONObject doc) {
        this.specification = specification;
        this.namespace = namespace;
        this.entity = entity;
        this.id = id;
        this.doc = doc;
    }

    static String toUnderscoreDelimitedUpperCase(String s) {
        CharBuffer source = CharBuffer.wrap(s);
        CharBuffer target = CharBuffer.allocate(source.remaining() * 2);
        boolean skip = false;
        for (int n = 0; source.hasRemaining(); n++) {
            char c = source.get();
            if (n > 0 && (Character.isUpperCase(c))) {
                if (!skip) target.append('_');
            } else if (n > 0 && (c == '/' || c == '#')) {
                target.append('_');
                skip = true;
                continue;
            }
            target.append(Character.toUpperCase(c));
            skip = false;
        }
        target.flip();
        return target.toString();
    }

    public Set<OutgoingLink> parse() {
        Set<OutgoingLink> outgoingLinks = new LinkedHashSet<>();

        JsonDocumentTraversal.walk(specification, entity, doc, (ancestors, jte) -> {
            if (!jte.isReference()) {
                return;
            }
            if (!jte.isValueOrArrayValue()) {
                return;
            }
            if (JSONObject.NULL.equals(jte.value)) {
                return;
            }

            String resourceURL = String.format("/%s/%s/%s", namespace, entity, id);
            String jsonKey = jte.isArrayValue() ? ancestors.getLast().key : jte.key;

            String relationshipName = toUnderscoreDelimitedUpperCase(jsonKey) + "_HAS_REF_TO";
            String relativeResourcePath = (JsonElementType.VALUE.equals(jte.type) ?
                    Arrays.asList(jte.uri(ancestors)).stream().collect(Collectors.joining("/")).concat(String.valueOf(jte.value)) :
                    Arrays.asList(jte.uri(ancestors)).stream().collect(Collectors.joining("/")));
            String relationshipURI = String.format("%s/%s", resourceURL, relativeResourcePath);

            String edgeResourceURL = String.valueOf(jte.value);
            String[] edgeItems = edgeResourceURL.split("/");
            String edgeEntity = edgeItems[1];
            String edgeId = edgeItems[2];

            outgoingLinks.add(new OutgoingLink(null,
                    relationshipURI,
                    relationshipName,
                    namespace,
                    entity,
                    id,
                    edgeEntity,
                    edgeId));
        });

        return outgoingLinks;
    }
}
