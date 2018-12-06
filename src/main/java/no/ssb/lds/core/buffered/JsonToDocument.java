package no.ssb.lds.core.buffered;

import no.ssb.lds.api.persistence.FragmentType;
import no.ssb.lds.api.persistence.buffered.Document;
import no.ssb.lds.api.persistence.buffered.DocumentKey;
import no.ssb.lds.api.persistence.buffered.DocumentLeafNode;
import org.json.JSONArray;
import org.json.JSONObject;

import java.time.ZonedDateTime;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class JsonToDocument {

    final String namespace;
    final String entity;
    final String id;
    final ZonedDateTime version;
    final DocumentKey documentKey;
    final JSONObject jsonObject;
    final int fragmentCapacity;

    public JsonToDocument(String namespace, String entity, String id, ZonedDateTime version, JSONObject jsonObject, int fragmentCapacity) {
        this.namespace = namespace;
        this.entity = entity;
        this.id = id;
        this.version = version;
        this.jsonObject = jsonObject;
        this.fragmentCapacity = fragmentCapacity;
        documentKey = new DocumentKey(namespace, entity, id, version);
    }

    public Document toDocument() {
        Map<String, DocumentLeafNode> leafNodesByPath = new LinkedHashMap<>();
        Deque<String> parentPath = new LinkedList<>();
        parentPath.add("$");
        populateMapFromJson(parentPath, leafNodesByPath, jsonObject);
        return new Document(
                new DocumentKey(
                        namespace,
                        entity,
                        id,
                        version
                ),
                leafNodesByPath,
                false
        );
    }

    void populateMapFromJson(Deque<String> parentPath, Map<String, DocumentLeafNode> leafNodesByPath, Object object) {
        if (object == null || JSONObject.NULL.equals(object)) {
            String path = parentPath.stream().collect(Collectors.joining("."));
            leafNodesByPath.put(path, new DocumentLeafNode(documentKey, path, FragmentType.NULL, null, fragmentCapacity));
        } else if (object instanceof String) {
            String path = parentPath.stream().collect(Collectors.joining("."));
            leafNodesByPath.put(path, new DocumentLeafNode(documentKey, path, FragmentType.STRING, (String) object, fragmentCapacity));
        } else if (object instanceof Number) {
            String path = parentPath.stream().collect(Collectors.joining("."));
            leafNodesByPath.put(path, new DocumentLeafNode(documentKey, path, FragmentType.NUMERIC, object.toString(), fragmentCapacity));
        } else if (object instanceof Boolean) {
            String path = parentPath.stream().collect(Collectors.joining("."));
            leafNodesByPath.put(path, new DocumentLeafNode(documentKey, path, FragmentType.BOOLEAN, object.toString(), fragmentCapacity));
        } else if (object instanceof JSONArray) {
            JSONArray array = (JSONArray) object;
            if (array.isEmpty()) {
                String path = parentPath.stream().collect(Collectors.joining("."));
                leafNodesByPath.put(path, new DocumentLeafNode(documentKey, path, FragmentType.EMPTY_ARRAY, null, fragmentCapacity));
            } else {
                for (int i = 0; i < array.length(); i++) {
                    String originalLast = parentPath.removeLast();
                    String last = originalLast + "[" + i + "]";
                    parentPath.addLast(last);
                    populateMapFromJson(parentPath, leafNodesByPath, array.get(i));
                    parentPath.removeLast();
                    parentPath.addLast(originalLast);
                }
            }
        } else if (object instanceof List) {
            List array = (List) object;
            if (array.isEmpty()) {
                String path = parentPath.stream().collect(Collectors.joining("."));
                leafNodesByPath.put(path, new DocumentLeafNode(documentKey, path, FragmentType.EMPTY_ARRAY, null, fragmentCapacity));
            } else {
                for (int i = 0; i < array.size(); i++) {
                    String originalLast = parentPath.removeLast();
                    String last = originalLast + "[" + i + "]";
                    parentPath.addLast(last);
                    populateMapFromJson(parentPath, leafNodesByPath, array.get(i));
                    parentPath.removeLast();
                    parentPath.addLast(originalLast);
                }
            }
        } else if (object instanceof JSONObject) {
            JSONObject node = (JSONObject) object;
            if (node.isEmpty()) {
                String path = parentPath.stream().collect(Collectors.joining("."));
                leafNodesByPath.put(path, new DocumentLeafNode(documentKey, path, FragmentType.EMPTY_OBJECT, null, fragmentCapacity));
            } else {
                for (Map.Entry<String, Object> entry : node.toMap().entrySet()) {
                    parentPath.addLast(entry.getKey());
                    populateMapFromJson(parentPath, leafNodesByPath, entry.getValue());
                    parentPath.removeLast();
                }
            }
        } else if (object instanceof Map) {
            Map<String, Object> map = (Map<String, Object>) object;
            if (map.isEmpty()) {
                String path = parentPath.stream().collect(Collectors.joining("."));
                leafNodesByPath.put(path, new DocumentLeafNode(documentKey, path, FragmentType.EMPTY_OBJECT, null, fragmentCapacity));
            } else {
                for (Map.Entry<String, Object> entry : map.entrySet()) {
                    parentPath.addLast(entry.getKey());
                    populateMapFromJson(parentPath, leafNodesByPath, entry.getValue());
                    parentPath.removeLast();
                }
            }
        } else {
            throw new UnsupportedOperationException("Type " + object.getClass().getName() + " not supported for path " + parentPath);
        }
    }
}
