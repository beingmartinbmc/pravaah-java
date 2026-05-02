package io.github.beingmartinbmc.pravaah.schema;

import io.github.beingmartinbmc.pravaah.*;
import io.github.beingmartinbmc.pravaah.internal.text.CsvFormat;
import io.github.beingmartinbmc.pravaah.internal.util.Maps;

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
            if (options != null && options.isDropBlankRows() && isBlankRow(cleaned)) {
                continue;
            }
            if (options == null || options.getDedupeKey() == null) {
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

    public static boolean isBlankRow(Row row) {
        if (row == null || row.isEmpty()) return true;
        for (Object value : row.values()) {
            if (value == null) continue;
            if (value instanceof String && ((String) value).trim().isEmpty()) continue;
            return false;
        }
        return true;
    }

    public static List<PravaahIssue> validateHeaders(Collection<String> headers, SchemaDefinition definition,
                                                     CleaningOptions cleaning) {
        List<PravaahIssue> issues = new ArrayList<>();
        if (headers == null || definition == null) return issues;

        Set<String> recognized = new HashSet<>();
        Map<String, String> aliasToCanonical = new HashMap<>();
        for (String key : definition.keySet()) {
            String normalized = normalizeHeader(key);
            recognized.add(normalized);
            aliasToCanonical.put(normalized, key);
        }

        Map<String, List<String>> fuzzy = cleaning == null ? null : cleaning.getFuzzyHeaders();
        if (fuzzy != null) {
            for (Map.Entry<String, List<String>> entry : fuzzy.entrySet()) {
                String canonical = entry.getKey();
                String canonicalNorm = normalizeHeader(canonical);
                recognized.add(canonicalNorm);
                aliasToCanonical.put(canonicalNorm, canonical);
                for (String alias : entry.getValue()) {
                    String normalizedAlias = normalizeHeader(alias);
                    recognized.add(normalizedAlias);
                    aliasToCanonical.put(normalizedAlias, canonical);
                }
            }
        }

        Set<String> presentCanonical = new HashSet<>();
        for (String header : headers) {
            String normalized = normalizeHeader(header == null ? "" : header);
            String canonical = aliasToCanonical.get(normalized);
            if (canonical != null) {
                presentCanonical.add(canonical);
            }
            if (!recognized.contains(normalized)) {
                issues.add(PravaahIssue.error("unexpected_header", "unexpected header: " + header,
                        0, header, header, "known schema header"));
            }
        }

        for (Map.Entry<String, FieldDefinition> entry : definition.entrySet()) {
            FieldDefinition field = entry.getValue();
            if (!field.isOptional() && !field.hasDefaultValue() && !presentCanonical.contains(entry.getKey())) {
                issues.add(PravaahIssue.error("missing_header", "missing header: " + entry.getKey(),
                        0, entry.getKey(), null, "required schema header"));
            }
        }
        return issues;
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
                        rowNumber, key, raw, expected(field)));
                continue;
            }

            String constraintIssue = validateConstraints(coerced.getValue(), field);
            if (constraintIssue != null) {
                issues = addIssue(issues, PravaahIssue.error("invalid_value", constraintIssue,
                        rowNumber, key, raw, expected(field)));
                continue;
            }

            if (field.getValidate() != null) {
                String customIssue = field.getValidate().apply(coerced.getValue(), row);
                if (customIssue != null) {
                    issues = addIssue(issues, PravaahIssue.error("invalid_value", customIssue,
                            rowNumber, key, raw, expected(field)));
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
            if (cleaning != null && cleaning.isDropBlankRows() && isBlankRow(cleaned)) {
                rowNumber++;
                continue;
            }
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
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(destination))) {
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
            case REGEX: {
                String value = String.valueOf(raw);
                Pattern regex = field.getRegex();
                return regex != null && regex.matcher(value).matches()
                        ? CoerceResult.success(value)
                        : CoerceResult.failure();
            }
            case ONE_OF: {
                Set<Object> allowed = field.getAllowedValues();
                if (allowed == null || allowed.isEmpty()) return CoerceResult.failure();
                if (allowed.contains(raw)) return CoerceResult.success(raw);
                if (field.isCoerce()) {
                    String rawText = String.valueOf(raw);
                    for (Object allowedValue : allowed) {
                        if (Objects.equals(rawText, String.valueOf(allowedValue))) {
                            return CoerceResult.success(allowedValue);
                        }
                    }
                }
                return CoerceResult.failure();
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

    private static String validateConstraints(Object value, FieldDefinition field) {
        if ((field.getMin() != null || field.getMax() != null) && value instanceof Number) {
            double n = ((Number) value).doubleValue();
            if (field.getMin() != null && n < field.getMin()) {
                return "value must be >= " + field.getMin();
            }
            if (field.getMax() != null && n > field.getMax()) {
                return "value must be <= " + field.getMax();
            }
        }
        return null;
    }

    private static String expected(FieldDefinition field) {
        if (field.getKind() == FieldKind.REGEX && field.getRegexSource() != null) {
            return "regex:" + field.getRegexSource();
        }
        if (field.getKind() == FieldKind.ONE_OF && field.getAllowedValues() != null) {
            return "one_of:" + field.getAllowedValues();
        }
        if (field.getMin() != null || field.getMax() != null) {
            StringBuilder sb = new StringBuilder(field.getKind().name().toLowerCase());
            sb.append(" range[");
            sb.append(field.getMin() == null ? "" : field.getMin());
            sb.append(",");
            sb.append(field.getMax() == null ? "" : field.getMax());
            sb.append("]");
            return sb.toString();
        }
        return field.getKind().name().toLowerCase();
    }

    private static int mapCapacity(int entries) {
        return Maps.mapCapacity(entries);
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
        return CsvFormat.csvEscape(value);
    }
}
