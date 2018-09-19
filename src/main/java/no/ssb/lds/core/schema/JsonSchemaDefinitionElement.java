package no.ssb.lds.core.schema;

import java.util.Map;
import java.util.Set;

public class JsonSchemaDefinitionElement {

    /**
     * The legal json types of this element, possible values:
     * object, array, string, number, boolean, null.
     */
    public final String[] types;

    /**
     * The title.
     */
    public final String title;

    /**
     * Format when type is one of: .
     */
    public final String format;

    /**
     * Used by object type only
     */
    public final Map<String, JsonSchemaDefinitionElement> properties;

    /**
     * Used by array type only.
     */
    public final JsonSchemaDefinitionElement items;

    /**
     * Mandatory properties when type is object.
     */
    public final Set<String> required;

    JsonSchemaDefinitionElement(
            String[] types,
            String title,
            String format,
            Map<String, JsonSchemaDefinitionElement> properties,
            JsonSchemaDefinitionElement items,
            Set<String> required) {
        this.types = types;
        this.title = title;
        this.format = format;
        this.properties = properties;
        this.items = items;
        this.required = required;
    }
}
