package no.ssb.lds.core.schema;

import java.util.Map;
import java.util.Set;

public class JsonSchemaDefinitionElement {

    /**
     * Non standard. Used to get the type name of $ref items. This is because every
     * type in graphql need a name.
     * Ideally, we should implement some sort of dereferencing but that's out
     * of scope ATM.
     */
    public String name;

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
     * The description.
     */
    public final String description;

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
            String description,
            String format,
            Map<String, JsonSchemaDefinitionElement> properties,
            JsonSchemaDefinitionElement items,
            Set<String> required) {
        this.types = types;
        this.title = title;
        this.description = description;
        this.format = format;
        this.properties = properties;
        this.items = items;
        this.required = required;
    }
}
