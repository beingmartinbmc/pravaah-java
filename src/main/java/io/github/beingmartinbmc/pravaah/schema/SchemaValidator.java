package io.github.beingmartinbmc.pravaah.schema;

import io.github.beingmartinbmc.pravaah.*;

import java.io.*;
import java.time.*;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.regex.Pattern;

public final class SchemaValidator {

    private static final Pattern EMAIL_PATTERN = Pattern.compile("^[^\\s@]+@[^\\s@]+\\.[^\\s@]+$");
    private static final Pattern NON_DIGIT = Pattern.compile("[^\\d+]");

    private SchemaValidator() {}

    public static String normalizeHeader(String value) {
        return value.trim()
                .toLowerCase()
                .replaceAll("[_\\-]+", " ")
                .replaceAll("\\s+", " ");
    }

    public static Row applyFuzzyHeaders(Row row, Map<String, List<String>> aliases) {
        if (aliases == null || aliases.isEmpty()) return row;

        Map<String, String> normalizedEntries = new LinkedHashMap<>();
        for (String key : row.keySet()) {
            normalizedEntries.put(normalizeHeader(key), key);
        }

        Row next = row.copy();
        for (Map.Entry<String, List<String>> entry : aliases.entrySet()) {
            String canonical = entry.getKey();
            if (next.containsKey(canonical)) continue;
            for (String name : entry.getValue()) {
                String existing = normalizedEntries.get(normalizeHeader(name));
                if (existing != null) {
                    next.put(canonical, next.get(existing));
                    break;
                }
            }
        }
        return next;
    }

    public static Row cleanRow(Row row, CleaningOptions options) {
        if (options == null) return row;
        Row withHeaders = applyFuzzyHeaders(row, options.getFuzzyHeaders());
        Row cleaned = new Row();
        for (Map.Entry<String, Object> entry : withHeaders.entrySet()) {
            Object value = entry.getValue();
            if (value instanceof String) {
                String s = (String) value;
                if (options.isTrim()) s = s.trim();
                if (options.isNormalizeWhitespace()) s = s.replaceAll("\\s+", " ");
                cleaned.put(entry.getKey(), s);
            } else {
                cleaned.put(entry.getKey(), value);
            }
        }
        return cleaned;
    }

    public static List<Row> cleanRows(Iterable<Row> rows, CleaningOptions options) {
        Set<String> seen = new HashSet<>();
        List<Row> output = new ArrayList<>();
        for (Row row : rows) {
            Row cleaned = cleanRow(row, options);
            if (options.getDedupeKey() == null) {
                output.add(cleaned);
                continue;
            }
            String identity = buildIdentity(cleaned, options.getDedupeKey());
            if (seen.add(identity)) {
                output.add(cleaned);
            }
        }
        return output;
    }

    public static ValidationResult validateRow(Row row, SchemaDefinition definition, int rowNumber) {
        List<PravaahIssue> issues = null;
        Row parsed = new Row(mapCapacity(definition.size()));

        for (Map.Entry<String, FieldDefinition> entry : definition.entrySet()) {
            String key = entry.getKey();
            FieldDefinition field = entry.getValue();
            Object raw = row.get(key);

            if (raw == null || "".equals(raw)) {
                if (field.hasDefaultValue()) {
                    parsed.put(key, field.getDefaultValue());
                } else if (field.isOptional()) {
                    parsed.put(key, null);
                } else {
                    issues = addIssue(issues, PravaahIssue.error("missing_column", key + " is required",
                            rowNumber, key, raw, field.getKind().name().toLowerCase()));
                }
                continue;
            }

            CoerceResult coerced = coerceValue(raw, field);
            if (!coerced.isOk()) {
                issues = addIssue(issues, PravaahIssue.error("invalid_type", key + " must be " + field.getKind().name().toLowerCase(),
                        rowNumber, key, raw, field.getKind().name().toLowerCase()));
                continue;
            }

            if (field.getValidate() != null) {
                String customIssue = field.getValidate().apply(coerced.getValue(), row);
                if (customIssue != null) {
                    issues = addIssue(issues, PravaahIssue.error("invalid_value", customIssue,
                            rowNumber, key, raw, field.getKind().name().toLowerCase()));
                    continue;
                }
            }

            parsed.put(key, coerced.getValue());
        }

        return new ValidationResult(issues == null ? parsed : null,
                issues == null ? Collections.emptyList() : issues);
    }

    public static ProcessResult validateRows(Iterable<Row> rows, SchemaDefinition definition,
                                              ValidationMode mode, CleaningOptions cleaning) {
        List<Row> output = new ArrayList<>(collectionSize(rows));
        List<PravaahIssue> issues = new ArrayList<>();
        int rowNumber = 1;

        for (Row row : rows) {
            Row cleaned = cleanRow(row, cleaning);
            ValidationResult result = validateRow(cleaned, definition, rowNumber);
            if (result.getValue() != null) {
                output.add(result.getValue());
            } else if (mode != ValidationMode.SKIP) {
                issues.addAll(result.getIssues());
                if (mode == ValidationMode.FAIL_FAST) {
                    throw new PravaahValidationException(result.getIssues());
                }
            }
            rowNumber++;
        }

        return new ProcessResult(output, issues, null);
    }

    private static int collectionSize(Iterable<Row> rows) {
        return rows instanceof Collection ? ((Collection<?>) rows).size() : 16;
    }

    public static void writeIssueReport(List<PravaahIssue> issues, String destination) throws IOException {
        BufferedWriter writer = new BufferedWriter(new FileWriter(destination));
        try {
            writer.write("severity,code,message,rowNumber,column,expected,rawValue");
            writer.newLine();
            for (PravaahIssue issue : issues) {
                writer.write(csvEscape(issue.getSeverity().name().toLowerCase()));
                writer.write(',');
                writer.write(csvEscape(issue.getCode()));
                writer.write(',');
                writer.write(csvEscape(issue.getMessage()));
                writer.write(',');
                writer.write(csvEscape(issue.getRowNumber() == null ? "" : String.valueOf(issue.getRowNumber())));
                writer.write(',');
                writer.write(csvEscape(issue.getColumn() == null ? "" : issue.getColumn()));
                writer.write(',');
                writer.write(csvEscape(issue.getExpected() == null ? "" : issue.getExpected()));
                writer.write(',');
                writer.write(csvEscape(stringifyIssueValue(issue.getRawValue())));
                writer.newLine();
            }
        } finally {
            writer.close();
        }
    }

    public static class ValidationResult {
        private final Row value;
        private final List<PravaahIssue> issues;

        public ValidationResult(Row value, List<PravaahIssue> issues) {
            this.value = value;
            this.issues = issues;
        }

        public Row getValue() { return value; }
        public List<PravaahIssue> getIssues() { return issues; }
    }

    private static class CoerceResult {
        private final boolean ok;
        private final Object value;

        CoerceResult(boolean ok, Object value) {
            this.ok = ok;
            this.value = value;
        }

        static CoerceResult success(Object value) { return new CoerceResult(true, value); }
        static CoerceResult failure() { return new CoerceResult(false, null); }

        boolean isOk() { return ok; }
        Object getValue() { return value; }
    }

    static CoerceResult coerceValue(Object raw, FieldDefinition field) {
        switch (field.getKind()) {
            case ANY:
                return CoerceResult.success(raw);
            case STRING:
                return CoerceResult.success(String.valueOf(raw));
            case EMAIL: {
                String email = String.valueOf(raw).trim();
                return EMAIL_PATTERN.matcher(email).matches() ? CoerceResult.success(email) : CoerceResult.failure();
            }
            case PHONE: {
                String phone = NON_DIGIT.matcher(String.valueOf(raw)).replaceAll("");
                return phone.length() >= 7 ? CoerceResult.success(phone) : CoerceResult.failure();
            }
            case NUMBER: {
                if (raw instanceof Number) return CoerceResult.success(((Number) raw).doubleValue());
                if (!field.isCoerce()) return CoerceResult.failure();
                try {
                    double d = Double.parseDouble(String.valueOf(raw));
                    if (Double.isFinite(d)) return CoerceResult.success(d);
                    return CoerceResult.failure();
                } catch (NumberFormatException e) {
                    return CoerceResult.failure();
                }
            }
            case BOOLEAN: {
                if (raw instanceof Boolean) return CoerceResult.success(raw);
                if (!field.isCoerce()) return CoerceResult.failure();
                String normalized = String.valueOf(raw).trim();
                if ("true".equalsIgnoreCase(normalized) || "1".equals(normalized)
                        || "yes".equalsIgnoreCase(normalized) || "y".equalsIgnoreCase(normalized)) {
                    return CoerceResult.success(true);
                }
                if ("false".equalsIgnoreCase(normalized) || "0".equals(normalized)
                        || "no".equalsIgnoreCase(normalized) || "n".equalsIgnoreCase(normalized)) {
                    return CoerceResult.success(false);
                }
                return CoerceResult.failure();
            }
            case DATE: {
                if (raw instanceof LocalDate || raw instanceof LocalDateTime || raw instanceof Instant) {
                    return CoerceResult.success(raw);
                }
                if (!field.isCoerce()) return CoerceResult.failure();
                try {
                    return CoerceResult.success(LocalDate.parse(String.valueOf(raw)));
                } catch (DateTimeParseException e) {
                    try {
                        return CoerceResult.success(Instant.parse(String.valueOf(raw)));
                    } catch (DateTimeParseException e2) {
                        return CoerceResult.failure();
                    }
                }
            }
            default:
                return CoerceResult.failure();
        }
    }

    public static boolean isDuplicate(Row row, List<String> dedupeKey, Set<String> seen) {
        if (dedupeKey == null) return false;
        String identity = buildIdentity(row, dedupeKey);
        return !seen.add(identity);
    }

    private static List<PravaahIssue> addIssue(List<PravaahIssue> issues, PravaahIssue issue) {
        if (issues == null) {
            issues = new ArrayList<>(1);
        }
        issues.add(issue);
        return issues;
    }

    private static int mapCapacity(int entries) {
        return Math.max(4, (int) (entries / 0.75f) + 1);
    }

    private static String buildIdentity(Row row, List<String> keys) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < keys.size(); i++) {
            if (i > 0) sb.append('\0');
            Object v = row.get(keys.get(i));
            sb.append(v == null ? "" : String.valueOf(v));
        }
        return sb.toString();
    }

    private static String stringifyIssueValue(Object value) {
        if (value == null) return "";
        if (value instanceof Instant) return value.toString();
        if (value instanceof LocalDate) return value.toString();
        if (value instanceof Map || value instanceof List) return String.valueOf(value);
        return String.valueOf(value);
    }

    static String csvEscape(String value) {
        if (value.contains(",") || value.contains("\"") || value.contains("\n")) {
            return "\"" + value.replace("\"", "\"\"") + "\"";
        }
        return value;
    }
}
