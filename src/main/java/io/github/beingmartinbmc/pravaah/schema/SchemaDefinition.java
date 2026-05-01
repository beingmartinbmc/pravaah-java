package io.github.beingmartinbmc.pravaah.schema;

import java.util.*;

/**
 * An ordered map of column names to their field definitions.
 */
public class SchemaDefinition extends LinkedHashMap<String, FieldDefinition> {

    public SchemaDefinition() {
        super();
    }

    public SchemaDefinition field(String name, FieldDefinition definition) {
        put(name, definition);
        return this;
    }

    public static SchemaDefinition of(String k1, FieldDefinition v1) {
        SchemaDefinition sd = new SchemaDefinition();
        sd.put(k1, v1);
        return sd;
    }

    public static SchemaDefinition of(String k1, FieldDefinition v1, String k2, FieldDefinition v2) {
        SchemaDefinition sd = new SchemaDefinition();
        sd.put(k1, v1);
        sd.put(k2, v2);
        return sd;
    }

    public static SchemaDefinition of(String k1, FieldDefinition v1, String k2, FieldDefinition v2,
                                       String k3, FieldDefinition v3) {
        SchemaDefinition sd = new SchemaDefinition();
        sd.put(k1, v1);
        sd.put(k2, v2);
        sd.put(k3, v3);
        return sd;
    }
}
