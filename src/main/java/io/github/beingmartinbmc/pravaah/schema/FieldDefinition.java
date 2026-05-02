package io.github.beingmartinbmc.pravaah.schema;

import io.github.beingmartinbmc.pravaah.Row;
import java.util.*;
import java.util.function.BiFunction;
import java.util.regex.Pattern;

public class FieldDefinition {
    private final FieldKind kind;
    private boolean optional;
    private Object defaultValue;
    private boolean hasDefault;
    private boolean coerce = true;
    private BiFunction<Object, Row, String> validate;
    private Pattern regex;
    private String regexSource;
    private Set<Object> allowedValues;
    private Double min;
    private Double max;

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

    public Pattern getRegex() { return regex; }
    public String getRegexSource() { return regexSource; }
    public FieldDefinition regex(String pattern) {
        this.regexSource = pattern;
        this.regex = Pattern.compile(pattern);
        return this;
    }
    public FieldDefinition regex(Pattern pattern) {
        this.regex = pattern;
        this.regexSource = pattern == null ? null : pattern.pattern();
        return this;
    }

    public Set<Object> getAllowedValues() { return allowedValues; }
    public FieldDefinition oneOf(Object... values) {
        this.allowedValues = new LinkedHashSet<>(Arrays.asList(values));
        return this;
    }
    public FieldDefinition oneOf(Collection<?> values) {
        this.allowedValues = new LinkedHashSet<Object>(values);
        return this;
    }

    public Double getMin() { return min; }
    public Double getMax() { return max; }
    public FieldDefinition range(double min, double max) {
        this.min = min;
        this.max = max;
        return this;
    }
}
