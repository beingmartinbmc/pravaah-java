package io.github.beingmartinbmc.pravaah;

import java.util.*;

/**
 * A thin wrapper around a {@link LinkedHashMap} that represents a single data row
 * with string column names and arbitrary cell values, preserving insertion order.
 */
public class Row extends LinkedHashMap<String, Object> {

    public Row() {
        super();
    }

    public Row(int initialCapacity) {
        super(initialCapacity);
    }

    public Row(Map<String, Object> map) {
        super(map);
    }

    public static Row of(String key, Object value) {
        Row row = new Row();
        row.put(key, value);
        return row;
    }

    public static Row of(String k1, Object v1, String k2, Object v2) {
        Row row = new Row();
        row.put(k1, v1);
        row.put(k2, v2);
        return row;
    }

    public static Row of(String k1, Object v1, String k2, Object v2, String k3, Object v3) {
        Row row = new Row();
        row.put(k1, v1);
        row.put(k2, v2);
        row.put(k3, v3);
        return row;
    }

    public static Row of(Object... keyValues) {
        if (keyValues.length % 2 != 0) {
            throw new IllegalArgumentException("Must provide key-value pairs");
        }
        Row row = new Row();
        for (int i = 0; i < keyValues.length; i += 2) {
            row.put(String.valueOf(keyValues[i]), keyValues[i + 1]);
        }
        return row;
    }

    public String getString(String key) {
        Object v = get(key);
        return v == null ? null : String.valueOf(v);
    }

    public Number getNumber(String key) {
        Object v = get(key);
        if (v instanceof Number) return (Number) v;
        if (v instanceof String) {
            try {
                return Double.parseDouble((String) v);
            } catch (NumberFormatException e) {
                return null;
            }
        }
        return null;
    }

    public Boolean getBoolean(String key) {
        Object v = get(key);
        if (v instanceof Boolean) return (Boolean) v;
        if (v instanceof String) {
            String s = ((String) v).trim().toLowerCase();
            if ("true".equals(s) || "1".equals(s) || "yes".equals(s) || "y".equals(s)) return true;
            if ("false".equals(s) || "0".equals(s) || "no".equals(s) || "n".equals(s)) return false;
        }
        return null;
    }

    public Row copy() {
        return new Row(this);
    }
}
