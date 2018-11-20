package no.ssb.lds.core.schema;

import org.everit.json.schema.Schema;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

public class JsonSchema {

    final Map<String, Schema> schemas = new LinkedHashMap<>();
    final Map<String, String> schemaJsonByName = new LinkedHashMap<>();
    final Map<String, JsonSchemaDefinitionElement> definitions = new LinkedHashMap<>();

    JsonSchema() {
    }

    public Set<String> getSchemaNames() {
        return new LinkedHashSet<>(schemaJsonByName.keySet());
    }

    public Map<String, JsonSchemaDefinitionElement> getDefinitions() {
        return definitions;
    }

    JsonSchema addSchema(String name, Schema schema) {
        schemas.put(name, schema);
        return this;
    }

    public Schema getSchema(String name) {
        return schemas.get(name);
    }

    JsonSchema addSchemaJson(String name, String schemaJson) {
        schemaJsonByName.put(name, schemaJson);
        return this;
    }

    public String getSchemaJson(String name) {
        return schemaJsonByName.get(name);
    }

    JsonSchema addDefinition(String definitionName, JsonSchemaDefinitionElement jsde) {
        jsde.name = definitionName;
        definitions.put(definitionName, jsde);
        return this;
    }
}
