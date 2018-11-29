package no.ssb.lds.core.buffered;

import no.ssb.lds.api.persistence.FragmentType;
import no.ssb.lds.api.persistence.buffered.Document;
import no.ssb.lds.api.persistence.buffered.DocumentLeafNode;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DocumentToJson {

    static final Pattern arrayNavigationPattern = Pattern.compile("([^\\[]*)\\[([0-9]+)\\]");

    final Document document;

    public DocumentToJson(Document document) {
        this.document = document;
    }

    public JSONObject toJSONObject() {
        JSONObject root = new JSONObject();
        for (Map.Entry<String, DocumentLeafNode> entry : document.leafNodesByPath().entrySet()) {
            String path = entry.getKey();
            DocumentLeafNode leafNode = entry.getValue();
            String[] pathElements = path.split("\\.");
            JSONObject parentOfLeaf = root;
            for (int i = 1; i < pathElements.length - 1; i++) {
                String pathElement = pathElements[i];
                Matcher m = arrayNavigationPattern.matcher(pathElement);
                if (m.matches()) {
                    // array-navigation
                    String arrayIdentifier = m.group(1);
                    int arrayIndex = Integer.parseInt(m.group(2));
                    if (!parentOfLeaf.has(arrayIdentifier)) {
                        parentOfLeaf.put(arrayIdentifier, new JSONArray());
                    }
                    JSONArray array = parentOfLeaf.getJSONArray(arrayIdentifier);
                    parentOfLeaf = new JSONObject();
                    array.put(arrayIndex, parentOfLeaf);
                } else {
                    // map
                    if (!parentOfLeaf.has(pathElement)) {
                        parentOfLeaf.put(pathElement, new JSONObject());
                    }
                    parentOfLeaf = parentOfLeaf.getJSONObject(pathElement);
                }
            }

            String leafPathElement = pathElements[pathElements.length - 1];
            Matcher m = arrayNavigationPattern.matcher(leafPathElement);
            if (m.matches()) {
                // array-navigation
                String arrayIdentifier = m.group(1);
                int arrayIndex = Integer.parseInt(m.group(2));
                if (!parentOfLeaf.has(arrayIdentifier)) {
                    parentOfLeaf.put(arrayIdentifier, new JSONArray());
                }
                JSONArray leaf = parentOfLeaf.getJSONArray(arrayIdentifier);

                if (FragmentType.NUMERIC == leafNode.type()) {
                    String strValue = (String) leafNode.value();
                    Object value;
                    // TODO Use pattern matching to find correct type rather than using exceptions
                    // TODO to control type and flow.
                    try {
                        value = Integer.valueOf(strValue);
                    } catch (NumberFormatException e) {
                        try {
                            value = Long.valueOf(strValue);
                        } catch (NumberFormatException e1) {
                            try {
                                value = Double.valueOf(strValue);
                            } catch (NumberFormatException e2) {
                                throw e2;
                            }
                        }
                    }
                    leaf.put(arrayIndex, value);
                } else if (FragmentType.STRING == leafNode.type()) {
                    leaf.put(arrayIndex, leafNode.value());
                } else if (FragmentType.BOOLEAN == leafNode.type()) {
                    leaf.put(arrayIndex, leafNode.value());
                } else if (FragmentType.EMPTY_ARRAY == leafNode.type()) {
                    leaf.put(arrayIndex, new JSONArray());
                } else if (FragmentType.EMPTY_OBJECT == leafNode.type()) {
                    leaf.put(arrayIndex, new JSONObject());
                } else if (FragmentType.NULL == leafNode.type()) {
                    leaf.put(arrayIndex, JSONObject.NULL);
                } else {
                    throw new UnsupportedOperationException("Unsupported FragmentType: " + leafNode.type());
                }
            } else {
                // map
                if (FragmentType.NUMERIC == leafNode.type()) {
                    String strValue = (String) leafNode.value();
                    Object value;
                    // TODO Use pattern matching to find correct type rather than using exceptions
                    // TODO to control type and flow.
                    try {
                        value = Integer.valueOf(strValue);
                    } catch (NumberFormatException e) {
                        try {
                            value = Long.valueOf(strValue);
                        } catch (NumberFormatException e1) {
                            try {
                                value = Double.valueOf(strValue);
                            } catch (NumberFormatException e2) {
                                throw e2;
                            }
                        }
                    }
                    parentOfLeaf.put(leafPathElement, value);
                } else if (FragmentType.STRING == leafNode.type()) {
                    parentOfLeaf.put(leafPathElement, leafNode.value());
                } else if (FragmentType.BOOLEAN == leafNode.type()) {
                    parentOfLeaf.put(leafPathElement, leafNode.value());
                } else if (FragmentType.EMPTY_ARRAY == leafNode.type()) {
                    parentOfLeaf.put(leafPathElement, new JSONArray());
                } else if (FragmentType.EMPTY_OBJECT == leafNode.type()) {
                    parentOfLeaf.put(leafPathElement, new JSONObject());
                } else if (FragmentType.NULL == leafNode.type()) {
                    parentOfLeaf.put(leafPathElement, JSONObject.NULL);
                } else {
                    throw new UnsupportedOperationException("Unsupported FragmentType: " + leafNode.type());
                }
            }
        }
        return root;
    }
}
