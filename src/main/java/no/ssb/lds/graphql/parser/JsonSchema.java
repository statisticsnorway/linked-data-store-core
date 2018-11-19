package no.ssb.lds.graphql.parser;

import org.json.JSONArray;
import org.json.JSONObject;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class JsonSchema {

    private static final JsonSchema EMPTY = null;
    private static final JsonSchema FALSE = null;

    public JSONObject context;
    public Set<Type> type;
    public Map<String, JsonSchema> definitions;
    public Optional<String> title;
    public Optional<String> description;
    public Optional<Object> def;

    public JsonSchema additionalItems;
    public Set<JsonSchema> items;
    public Integer maxItems; // > 0
    public Integer minItems = 0;
    public Boolean uniqueItems = false;
    public Integer maxProperties; // > 0.
    public Integer minProperties = 0;
    public Set<String> required; // >= 1.

    public JsonSchema additionalProperties = EMPTY;
    public Boolean allowAdditionalProperties;

    public Map<String, JsonSchema> properties = new LinkedHashMap<>();

    public Map<String, JsonSchema> patternProperties = new LinkedHashMap<>();

    /*
     * 5.4.5.1.  Valid values
     *
     * This keyword's value MUST be an object.  Each value of this object
     * MUST be either an object or an array.
     *
     * If the value is an object, it MUST be a valid JSON Schema.  This is
     * called a schema dependency.
     *
     * If the value is an array, it MUST have at least one element.  Each
     * element MUST be a string, and elements in the array MUST be unique.
     * This is called a property dependency.
     *
     */
    public Map<String, JsonSchema> schemaDependency;
    public Map<String, Set<String>> propertyDependency;

    public Set<Object> enumProperty;

    public Set<JsonSchema> allOf;

    public Set<JsonSchema> anyOf;
    public Set<JsonSchema> oneOf;

    public JsonSchema not;

    public String format;

    public JsonSchema(JSONObject context) {
    }

    public Optional<String> ref() {
        return Optional.ofNullable(context.optString("$ref", null));
    }

    public Set<JsonSchema> allOf() {
        if (allOf == null) {
            this.allOf = new LinkedHashSet<>();
            JSONArray allOf = context.getJSONArray("allOf");
            if (allOf.length() < 1) {
                throw new IllegalArgumentException("allOf < 1");
            }
            for (int i = 0; i < allOf.length(); i++) {
                this.allOf.add(new JsonSchema(allOf.getJSONObject(i)));
            }
        }
        return this.allOf;
    }

    public Set<Type> types() {
        return null;
    }

    public enum Type {
        ARRAY("array"),
        BOOLEAN("boolean"),
        INTEGER("integer"),
        NUMBER("number"),
        NULL("null"),
        OBJECT("object"),
        STRING("string");

        private final String value;

        Type(String value) {
            this.value = value;
        }

        public static Type fromString(String type) {
            if (type == null) {
                return NULL;
            }
            for (Type value : Type.values()) {
                if (value.value.equalsIgnoreCase(type)) {
                    return value;
                }
            }
            throw new IllegalArgumentException();
        }

        public String getValue() {
            return this.value;
        }
    }
}
