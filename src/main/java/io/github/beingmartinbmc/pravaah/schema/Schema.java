package io.github.beingmartinbmc.pravaah.schema;

/**
 * Factory methods for building field definitions, mirroring the TypeScript {@code schema.*} builders.
 */
public final class Schema {

    private Schema() {}

    public static FieldDefinition string() {
        return new FieldDefinition(FieldKind.STRING).coerce(true);
    }

    public static FieldDefinition string(boolean optional) {
        return string().optional(optional);
    }

    public static FieldDefinition number() {
        return new FieldDefinition(FieldKind.NUMBER).coerce(true);
    }

    public static FieldDefinition number(boolean optional) {
        return number().optional(optional);
    }

    public static FieldDefinition bool() {
        return new FieldDefinition(FieldKind.BOOLEAN).coerce(true);
    }

    public static FieldDefinition bool(boolean optional) {
        return bool().optional(optional);
    }

    public static FieldDefinition date() {
        return new FieldDefinition(FieldKind.DATE).coerce(true);
    }

    public static FieldDefinition date(boolean optional) {
        return date().optional(optional);
    }

    public static FieldDefinition email() {
        return new FieldDefinition(FieldKind.EMAIL).coerce(true);
    }

    public static FieldDefinition email(boolean optional) {
        return email().optional(optional);
    }

    public static FieldDefinition phone() {
        return new FieldDefinition(FieldKind.PHONE).coerce(true);
    }

    public static FieldDefinition phone(boolean optional) {
        return phone().optional(optional);
    }

    public static FieldDefinition any() {
        return new FieldDefinition(FieldKind.ANY);
    }
}
