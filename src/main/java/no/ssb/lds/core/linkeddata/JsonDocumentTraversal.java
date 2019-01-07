package no.ssb.lds.core.linkeddata;

import no.ssb.lds.api.specification.Specification;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.function.BiConsumer;

final class JsonDocumentTraversal {
    private final Specification specification;
    private final String entity;
    private final Deque<JsonTraversalElement> ancestors = new LinkedList<>();
    private final BiConsumer<Deque<JsonTraversalElement>, JsonTraversalElement> visitor;

    private JsonDocumentTraversal(Specification specification, String entity, JSONObject jsonDocument, BiConsumer<Deque<JsonTraversalElement>, JsonTraversalElement> visitor) {
        this.specification = specification;
        this.entity = entity;
        this.visitor = visitor;
        walkJsonObject(jsonDocument);
    }

    static void walk(Specification specification, String entity, JSONObject jsonDocument, BiConsumer<Deque<JsonTraversalElement>, JsonTraversalElement> visitor) {
        new JsonDocumentTraversal(specification, entity, jsonDocument, visitor);
    }

    private void walkJsonObject(JSONObject jsonObject) {
        for (String key : jsonObject.keySet()) {
            Object value = jsonObject.get(key);

            if (value instanceof JSONObject) {
                JsonTraversalElement te = new JsonTraversalElement(key, value, JsonElementType.OBJECT);
                te.setSpecificationElement(specification.getElement(entity, te.uri(ancestors)));
                visitor.accept(ancestors, te);
                ancestors.addLast(te);
                walkJsonObject((JSONObject) value);
                ancestors.removeLast();

            } else if (value instanceof JSONArray) {
                JsonTraversalElement te = new JsonTraversalElement(key, value, JsonElementType.ARRAY);
                te.setSpecificationElement(specification.getElement(entity, te.uri(ancestors)));
                //visitor.accept(ancestors, te);
                Integer pos = 0;
                for (Iterator<Object> it = ((JSONArray) value).iterator(); it.hasNext(); ) {
                    Object arrayElement = it.next();
                    if (arrayElement instanceof JSONObject) {
                        walkJsonObject((JSONObject) arrayElement);
                    } else if (arrayElement instanceof JSONArray) {
                        throw new UnsupportedOperationException("Unsupported data type: " + arrayElement.getClass());
                    } else {
                        JsonTraversalElement teArray = new JsonTraversalElement(pos.toString(), arrayElement, JsonElementType.ARRAY_VALUE);
                        ancestors.addLast(te);
                        teArray.setSpecificationElement(te.getSpecificationElement());
                        visitor.accept(ancestors, teArray);
                        ancestors.removeLast();
                        pos++;
                    }
                }

            } else {
                JsonTraversalElement te = new JsonTraversalElement(key, value, JsonElementType.VALUE);
                te.setSpecificationElement(specification.getElement(entity, te.uri(ancestors)));
                visitor.accept(ancestors, te);
            }
        }
    }
}
