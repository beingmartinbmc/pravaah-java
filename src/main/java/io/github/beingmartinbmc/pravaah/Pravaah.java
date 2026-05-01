package io.github.beingmartinbmc.pravaah;

import io.github.beingmartinbmc.pravaah.csv.CsvReader;
import io.github.beingmartinbmc.pravaah.csv.CsvWriter;
import io.github.beingmartinbmc.pravaah.diff.DiffEngine;
import io.github.beingmartinbmc.pravaah.formula.FormulaEngine;
import io.github.beingmartinbmc.pravaah.perf.PerfUtils;
import io.github.beingmartinbmc.pravaah.pipeline.PravaahPipeline;
import io.github.beingmartinbmc.pravaah.query.QueryEngine;
import io.github.beingmartinbmc.pravaah.runtime.RuntimeSupport;
import io.github.beingmartinbmc.pravaah.schema.*;
import io.github.beingmartinbmc.pravaah.xlsx.XlsxReader;
import io.github.beingmartinbmc.pravaah.xlsx.XlsxWriter;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Main entry point for the Pravaah library. Provides static convenience methods
 * mirroring the TypeScript {@code read()}, {@code write()}, {@code parse()}, {@code parseDetailed()}.
 */
public final class Pravaah {

    private Pravaah() {}

    public static String runtimeImplementation() {
        return RuntimeSupport.implementation();
    }

    // --- Read ---

    public static PravaahPipeline read(String filePath) {
        return read(filePath, ReadOptions.defaults());
    }

    public static PravaahPipeline read(String filePath, ReadOptions options) {
        PravaahFormat format = options.getFormat() != null ? options.getFormat() : PravaahFormat.fromExtension(filePath);
        switch (format) {
            case CSV:
                return new PravaahPipeline(() -> CsvReader.readAll(filePath, options));
            case XLSX:
                return new PravaahPipeline(() -> XlsxReader.readAll(filePath, options));
            case JSON:
                return new PravaahPipeline(() -> readJsonFile(filePath));
            default:
                throw new IllegalArgumentException("Unsupported read format: " + format);
        }
    }

    public static PravaahPipeline read(byte[] data, ReadOptions options) {
        PravaahFormat format = options.getFormat();
        if (format == null) format = PravaahFormat.XLSX;
        switch (format) {
            case CSV:
                return new PravaahPipeline(() -> CsvReader.readAll(data, options));
            case XLSX:
                return new PravaahPipeline(() -> XlsxReader.readAll(data, options));
            case JSON:
                return new PravaahPipeline(() -> parseJsonRows(new String(data, StandardCharsets.UTF_8)));
            default:
                throw new IllegalArgumentException("Unsupported read format: " + format);
        }
    }

    public static PravaahPipeline read(List<Row> rows) {
        return new PravaahPipeline(() -> new ArrayList<>(rows));
    }

    // --- Write ---

    public static ProcessStats write(List<Row> rows, String destination) throws IOException {
        return write(rows, destination, WriteOptions.defaults());
    }

    public static ProcessStats write(List<Row> rows, String destination, WriteOptions options) throws IOException {
        ProcessStats stats = PerfUtils.createStats();
        stats.setRowsProcessed(rows.size());
        stats.setRowsWritten(rows.size());

        PravaahFormat format = options.getFormat() != null ? options.getFormat() : PravaahFormat.fromExtension(destination);
        switch (format) {
            case CSV:
                CsvWriter.write(rows, destination, options);
                break;
            case XLSX:
                XlsxWriter.writeRows(rows, destination, options);
                break;
            case JSON:
                writeJsonFile(rows, destination);
                break;
            default:
                throw new IllegalArgumentException("Unsupported write format: " + format);
        }

        return PerfUtils.finishStats(stats);
    }

    // --- Parse ---

    public static List<Row> parse(byte[] data, SchemaDefinition definition, ReadOptions options) throws IOException {
        PravaahPipeline pipeline = read(data, options);
        List<Row> raw = pipeline.collect();
        ValidationMode mode = options.getValidation() != null ? options.getValidation() : ValidationMode.COLLECT;
        ProcessResult result = SchemaValidator.validateRows(raw, definition, mode, options.getCleaning());
        return result.getRows();
    }

    public static List<Row> parse(String filePath, SchemaDefinition definition, ReadOptions options) throws IOException {
        List<Row> raw = read(filePath, options).collect();
        ValidationMode mode = options.getValidation() != null ? options.getValidation() : ValidationMode.COLLECT;
        ProcessResult result = SchemaValidator.validateRows(raw, definition, mode, options.getCleaning());
        return result.getRows();
    }

    // --- Parse Detailed ---

    public static ProcessResult parseDetailed(byte[] data, SchemaDefinition definition, ReadOptions options) throws IOException {
        List<Row> raw = read(data, options).collect();
        ProcessStats stats = PerfUtils.createStats();
        List<Row> validRows = new ArrayList<>(raw.size());
        List<PravaahIssue> issues = new ArrayList<>();
        int rowNumber = 1;
        Set<String> seen = new HashSet<>();

        ValidationMode mode = options.getValidation() != null ? options.getValidation() : ValidationMode.COLLECT;

        for (Row row : raw) {
            Row cleaned = SchemaValidator.cleanRow(row, options.getCleaning());
            if (options.getCleaning() != null && SchemaValidator.isDuplicate(cleaned,
                    options.getCleaning().getDedupeKey(), seen)) {
                rowNumber++;
                continue;
            }
            SchemaValidator.ValidationResult result = SchemaValidator.validateRow(cleaned, definition, rowNumber);
            stats.incrementRowsProcessed();
            if ((rowNumber & 4095) == 0) PerfUtils.observeMemory(stats);

            if (result.getValue() != null) {
                validRows.add(result.getValue());
            } else if (mode != ValidationMode.SKIP) {
                issues.addAll(result.getIssues());
                stats.addErrors(result.getIssues().size());
                if (mode == ValidationMode.FAIL_FAST) {
                    throw new PravaahValidationException(result.getIssues());
                }
            }
            rowNumber++;
        }

        return new ProcessResult(validRows, issues, PerfUtils.finishStats(stats));
    }

    public static ProcessResult parseDetailed(String filePath, SchemaDefinition definition, ReadOptions options) throws IOException {
        List<Row> raw = read(filePath, options).collect();
        return parseDetailed(raw, definition, options);
    }

    private static ProcessResult parseDetailed(List<Row> raw, SchemaDefinition definition, ReadOptions options) {
        ProcessStats stats = PerfUtils.createStats();
        List<Row> validRows = new ArrayList<>(raw.size());
        List<PravaahIssue> issues = new ArrayList<>();
        int rowNumber = 1;
        Set<String> seen = new HashSet<>();

        ValidationMode mode = options.getValidation() != null ? options.getValidation() : ValidationMode.COLLECT;

        for (Row row : raw) {
            Row cleaned = SchemaValidator.cleanRow(row, options.getCleaning());
            if (options.getCleaning() != null && SchemaValidator.isDuplicate(cleaned,
                    options.getCleaning().getDedupeKey(), seen)) {
                rowNumber++;
                continue;
            }
            SchemaValidator.ValidationResult result = SchemaValidator.validateRow(cleaned, definition, rowNumber);
            stats.incrementRowsProcessed();

            if (result.getValue() != null) {
                validRows.add(result.getValue());
            } else if (mode != ValidationMode.SKIP) {
                issues.addAll(result.getIssues());
                stats.addErrors(result.getIssues().size());
                if (mode == ValidationMode.FAIL_FAST) {
                    throw new PravaahValidationException(result.getIssues());
                }
            }
            rowNumber++;
        }

        return new ProcessResult(validRows, issues, PerfUtils.finishStats(stats));
    }

    // --- Convenience re-exports ---

    public static List<Row> query(List<Row> source, String sql) {
        return QueryEngine.query(source, sql);
    }

    public static DiffEngine.DiffResult diff(Iterable<Row> oldRows, Iterable<Row> newRows, String... keys) {
        return DiffEngine.diff(oldRows, newRows, keys);
    }

    public static Map<String, List<Row>> createIndex(Iterable<Row> rows, String... keys) {
        return QueryEngine.createIndex(rows, keys);
    }

    public static List<Row> joinRows(Iterable<Row> left, Iterable<Row> right, String... keys) {
        return QueryEngine.joinRows(left, right, keys);
    }

    public static Object evaluateFormula(String formula, Row row) {
        return FormulaEngine.evaluateFormula(formula, row);
    }

    // --- JSON helpers ---

    private static List<Row> readJsonFile(String path) throws IOException {
        BufferedReader reader = new BufferedReader(
                new InputStreamReader(new FileInputStream(path), StandardCharsets.UTF_8));
        try {
            StringBuilder sb = new StringBuilder();
            char[] buf = new char[4096];
            int read;
            while ((read = reader.read(buf)) != -1) sb.append(buf, 0, read);
            return parseJsonRows(sb.toString());
        } finally {
            reader.close();
        }
    }

    static List<Row> parseJsonRows(String json) {
        json = json.trim();
        if (!json.startsWith("[")) throw new IllegalArgumentException("Expected JSON array");

        List<Row> rows = new ArrayList<>();
        int i = 1;
        while (i < json.length()) {
            i = skipWhitespace(json, i);
            if (i >= json.length() || json.charAt(i) == ']') break;
            if (json.charAt(i) == ',') { i++; continue; }
            if (json.charAt(i) == '{') {
                int end = findMatchingBrace(json, i);
                String objStr = json.substring(i, end + 1);
                rows.add(parseJsonObject(objStr));
                i = end + 1;
            } else {
                i++;
            }
        }
        return rows;
    }

    private static Row parseJsonObject(String json) {
        Row row = new Row();
        json = json.trim();
        if (!json.startsWith("{") || !json.endsWith("}")) return row;
        json = json.substring(1, json.length() - 1).trim();
        if (json.isEmpty()) return row;

        int i = 0;
        while (i < json.length()) {
            i = skipWhitespace(json, i);
            if (i >= json.length()) break;
            if (json.charAt(i) == ',') { i++; continue; }
            if (json.charAt(i) != '"') break;

            int keyEnd = json.indexOf('"', i + 1);
            String key = json.substring(i + 1, keyEnd);
            i = keyEnd + 1;
            i = skipWhitespace(json, i);
            if (i < json.length() && json.charAt(i) == ':') i++;
            i = skipWhitespace(json, i);

            if (i >= json.length()) break;
            char c = json.charAt(i);
            if (c == '"') {
                int valEnd = findEndOfString(json, i);
                String val = json.substring(i + 1, valEnd).replace("\\\"", "\"").replace("\\\\", "\\");
                row.put(key, val);
                i = valEnd + 1;
            } else if (c == '{') {
                int end = findMatchingBrace(json, i);
                row.put(key, json.substring(i, end + 1));
                i = end + 1;
            } else if (json.startsWith("null", i)) {
                row.put(key, null);
                i += 4;
            } else if (json.startsWith("true", i)) {
                row.put(key, true);
                i += 4;
            } else if (json.startsWith("false", i)) {
                row.put(key, false);
                i += 5;
            } else {
                int end = i;
                while (end < json.length() && json.charAt(end) != ',' && json.charAt(end) != '}') end++;
                String numStr = json.substring(i, end).trim();
                try {
                    double d = Double.parseDouble(numStr);
                    if (d == Math.floor(d) && !numStr.contains(".") && d >= Integer.MIN_VALUE && d <= Integer.MAX_VALUE) {
                        row.put(key, (int) d);
                    } else {
                        row.put(key, d);
                    }
                } catch (NumberFormatException e) {
                    row.put(key, numStr);
                }
                i = end;
            }
        }
        return row;
    }

    private static int skipWhitespace(String s, int i) {
        while (i < s.length() && Character.isWhitespace(s.charAt(i))) i++;
        return i;
    }

    private static int findMatchingBrace(String s, int start) {
        int depth = 0;
        boolean inString = false;
        for (int i = start; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '\\' && inString) { i++; continue; }
            if (c == '"') inString = !inString;
            if (!inString) {
                if (c == '{') depth++;
                if (c == '}') { depth--; if (depth == 0) return i; }
            }
        }
        return s.length() - 1;
    }

    private static int findEndOfString(String s, int start) {
        for (int i = start + 1; i < s.length(); i++) {
            if (s.charAt(i) == '\\') { i++; continue; }
            if (s.charAt(i) == '"') return i;
        }
        return s.length() - 1;
    }

    private static void writeJsonFile(List<Row> rows, String destination) throws IOException {
        StringBuilder sb = new StringBuilder("[\n");
        for (int i = 0; i < rows.size(); i++) {
            if (i > 0) sb.append(",\n");
            sb.append("  ");
            Row row = rows.get(i);
            sb.append("{");
            boolean first = true;
            for (Map.Entry<String, Object> entry : row.entrySet()) {
                if (!first) sb.append(", ");
                sb.append("\"").append(entry.getKey().replace("\"", "\\\"")).append("\": ");
                Object v = entry.getValue();
                if (v == null) sb.append("null");
                else if (v instanceof Number) sb.append(v);
                else if (v instanceof Boolean) sb.append(v);
                else sb.append("\"").append(String.valueOf(v).replace("\"", "\\\"")).append("\"");
                first = false;
            }
            sb.append("}");
        }
        sb.append("\n]\n");
        FileWriter fw = new FileWriter(destination);
        try {
            fw.write(sb.toString());
        } finally {
            fw.close();
        }
    }
}
