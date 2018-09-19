package no.ssb.lds.core.schema;

import org.everit.json.schema.Schema;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class JsonSchema04Builder {

    JsonSchema jsonSchema;
    final String schemaName;
    final String schemaJson;

    public JsonSchema04Builder(JsonSchema jsonSchema, String schemaName, String schemaJson) {
        this.jsonSchema = jsonSchema;
        this.schemaName = schemaName;
        this.schemaJson = schemaJson;
    }

    public JsonSchema build() {
        if (jsonSchema == null) {
            jsonSchema = new JsonSchema();
        }
        JSONObject jsonObject = new JSONObject(schemaJson);
        jsonSchema.addSchemaJson(schemaName, schemaJson);
        if (jsonObject.has("definitions")) {
            JSONObject definitions = jsonObject.optJSONObject("definitions");
            for (String definitionName : definitions.keySet()) {
                JSONObject definition = definitions.getJSONObject(definitionName);
                if (!jsonSchema.getDefinitions().containsKey(definitionName)) {
                    JsonSchemaDefinitionElement jsde = buildElement(definitions, definition);
                    jsonSchema.addDefinition(definitionName, jsde);
                }
            }
        }
        // TODO remove link directives from schema
        Schema schema = SchemaLoader.load(jsonObject);
        jsonSchema.addSchema(schemaName, schema);
        return jsonSchema;
    }

    private JsonSchemaDefinitionElement buildElement(JSONObject definitions, JSONObject jsonElement) {
        if (jsonElement.has("$ref")) {
            String refValue = jsonElement.getString("$ref");
            Matcher m = Pattern.compile("#/definitions/(.*)").matcher(refValue);
            if (!m.matches()) {
                throw new IllegalStateException("$ref present but unable to resolve: " + refValue);
            }
            String definitionRef = m.group(1);
            if (!jsonSchema.getDefinitions().containsKey(definitionRef)) {
                JSONObject definition = definitions.getJSONObject(definitionRef);
                if (definition == null) {
                    throw new IllegalStateException("Referenced definition does not exist: " + refValue);
                }
                JsonSchemaDefinitionElement jsde = buildElement(definitions, definition);
                jsonSchema.addDefinition(definitionRef, jsde);
            }
            JsonSchemaDefinitionElement refElement = jsonSchema.getDefinitions().get(definitionRef);
            return refElement;
        }
        String[] types = new String[]{"object"};
        String type = jsonElement.optString("type", null);
        if (type != null) {
            types = new String[]{type};
        } else {
            JSONArray anyOfArray = jsonElement.optJSONArray("anyOf");
            if (anyOfArray != null) {
                Set<String> set = new LinkedHashSet<>();
                types = new String[anyOfArray.length()];
                for (int i = 0; i < types.length; i++) {
                    String anyOfType = anyOfArray.getJSONObject(i).optString("type", null);
                    if (anyOfType != null) {
                        set.add(anyOfType);
                    }
                }
                types = set.toArray(new String[set.size()]);
            }
        }
        String title = jsonElement.optString("title", null);
        String format = jsonElement.optString("format", null);
        Map<String, JsonSchemaDefinitionElement> properties = new LinkedHashMap<>();
        if (jsonElement.has("properties")) {
            JSONObject jsonObjectProperties = jsonElement.getJSONObject("properties");
            for (String propertyName : jsonObjectProperties.keySet()) {
                JSONObject property = jsonObjectProperties.getJSONObject(propertyName);
                JsonSchemaDefinitionElement propertyElement = buildElement(definitions, property);
                properties.put(propertyName, propertyElement);
            }
        }
        JsonSchemaDefinitionElement items = null;
        if (jsonElement.has("items")) {
            JSONObject itemsElement = jsonElement.getJSONObject("items");
            items = buildElement(definitions, itemsElement);
        }
        Set<String> required = new LinkedHashSet<>();
        if (jsonElement.has("required")) {
            JSONArray requiredJsonArray = jsonElement.getJSONArray("required");
            for (int i = 0; i < requiredJsonArray.length(); i++) {
                required.add(requiredJsonArray.getString(i));
            }
        }
        return new JsonSchemaDefinitionElement(
                types,
                title,
                format,
                properties,
                items,
                required
        );
    }
}
