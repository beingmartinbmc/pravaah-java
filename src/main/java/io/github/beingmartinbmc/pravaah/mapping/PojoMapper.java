package io.github.beingmartinbmc.pravaah.mapping;

import io.github.beingmartinbmc.pravaah.Row;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public final class PojoMapper {
    private PojoMapper() {}

    public static <T> List<T> mapRows(Iterable<Row> rows, Class<T> type) {
        List<T> result = new ArrayList<>();
        for (Row row : rows) {
            result.add(mapRow(row, type));
        }
        return result;
    }

    public static <T> T mapRow(Row row, Class<T> type) {
        try {
            Constructor<T> constructor = type.getDeclaredConstructor();
            constructor.setAccessible(true);
            T instance = constructor.newInstance();
            for (Field field : fields(type)) {
                String column = columnName(field);
                Object raw = row.get(column);
                if ((raw == null || "".equals(raw)) && field.isAnnotationPresent(Required.class)) {
                    throw new IllegalArgumentException("Missing required column: " + column);
                }
                if (raw == null || "".equals(raw)) continue;
                field.setAccessible(true);
                field.set(instance, convert(raw, field));
            }
            return instance;
        } catch (ReflectiveOperationException e) {
            throw new IllegalArgumentException("Cannot map row to " + type.getName(), e);
        }
    }

    private static List<Field> fields(Class<?> type) {
        List<Field> fields = new ArrayList<>();
        Class<?> current = type;
        while (current != null && current != Object.class) {
            for (Field field : current.getDeclaredFields()) {
                if (Modifier.isStatic(field.getModifiers())) continue;
                fields.add(field);
            }
            current = current.getSuperclass();
        }
        return fields;
    }

    private static String columnName(Field field) {
        Column column = field.getAnnotation(Column.class);
        if (column != null && !column.value().isEmpty()) return column.value();
        return field.getName();
    }

    private static Object convert(Object raw, Field field) {
        Class<?> target = field.getType();
        if (target.isInstance(raw)) return raw;
        String text = String.valueOf(raw).trim();
        if (target == String.class) return String.valueOf(raw);
        if (target == int.class || target == Integer.class) return number(raw).intValue();
        if (target == long.class || target == Long.class) return number(raw).longValue();
        if (target == double.class || target == Double.class) return number(raw).doubleValue();
        if (target == float.class || target == Float.class) return number(raw).floatValue();
        if (target == BigDecimal.class) return raw instanceof BigDecimal ? raw : new BigDecimal(text);
        if (target == boolean.class || target == Boolean.class) return bool(raw);
        if (target == LocalDate.class) {
            DateFormat format = field.getAnnotation(DateFormat.class);
            return format == null ? LocalDate.parse(text) : LocalDate.parse(text, DateTimeFormatter.ofPattern(format.value()));
        }
        if (target == LocalDateTime.class) {
            DateFormat format = field.getAnnotation(DateFormat.class);
            return format == null ? LocalDateTime.parse(text) : LocalDateTime.parse(text, DateTimeFormatter.ofPattern(format.value()));
        }
        if (target == Instant.class) return Instant.parse(text);
        if (target.isEnum()) {
            @SuppressWarnings({"unchecked", "rawtypes"})
            Object value = Enum.valueOf((Class<? extends Enum>) target.asSubclass(Enum.class), text);
            return value;
        }
        return raw;
    }

    private static Number number(Object raw) {
        if (raw instanceof Number) return (Number) raw;
        return Double.parseDouble(String.valueOf(raw).trim());
    }

    private static Boolean bool(Object raw) {
        if (raw instanceof Boolean) return (Boolean) raw;
        String text = String.valueOf(raw).trim();
        if ("true".equalsIgnoreCase(text) || "1".equals(text) || "yes".equalsIgnoreCase(text) || "y".equalsIgnoreCase(text)) {
            return true;
        }
        if ("false".equalsIgnoreCase(text) || "0".equals(text) || "no".equalsIgnoreCase(text) || "n".equalsIgnoreCase(text)) {
            return false;
        }
        throw new IllegalArgumentException("Cannot convert value to boolean: " + raw);
    }
}
