package io.github.beingmartinbmc.pravaah.schema;

import io.github.beingmartinbmc.pravaah.Row;
import java.util.function.BiFunction;

public class FieldDefinition {
    private final FieldKind kind;
    private boolean optional;
    private Object defaultValue;
    private boolean hasDefault;
    private boolean coerce = true;
    private BiFunction<Object, Row, String> validate;

    public FieldDefinition(FieldKind kind) {
        this.kind = kind;
    }

    public FieldKind getKind() { return kind; }

    public boolean isOptional() { return optional; }
    public FieldDefinition optional(boolean o) { this.optional = o; return this; }

    public Object getDefaultValue() { return defaultValue; }
    public boolean hasDefaultValue() { return hasDefault; }
    public FieldDefinition defaultValue(Object d) { this.defaultValue = d; this.hasDefault = true; return this; }

    public boolean isCoerce() { return coerce; }
    public FieldDefinition coerce(boolean c) { this.coerce = c; return this; }

    public BiFunction<Object, Row, String> getValidate() { return validate; }
    public FieldDefinition validate(BiFunction<Object, Row, String> v) { this.validate = v; return this; }
}
