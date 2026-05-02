package io.github.beingmartinbmc.pravaah;

import io.github.beingmartinbmc.pravaah.csv.CsvReader;
import io.github.beingmartinbmc.pravaah.csv.CsvWriter;
import io.github.beingmartinbmc.pravaah.csv.RowConsumer;
import io.github.beingmartinbmc.pravaah.diff.DiffEngine;
import io.github.beingmartinbmc.pravaah.formula.FormulaEngine;
import io.github.beingmartinbmc.pravaah.internal.io.IOUtils;
import io.github.beingmartinbmc.pravaah.internal.json.JsonRowWriter;
import io.github.beingmartinbmc.pravaah.mapping.PojoMapper;
import io.github.beingmartinbmc.pravaah.perf.PerfUtils;
import io.github.beingmartinbmc.pravaah.pipeline.PravaahPipeline;
import io.github.beingmartinbmc.pravaah.query.QueryEngine;
import io.github.beingmartinbmc.pravaah.runtime.RuntimeSupport;
import io.github.beingmartinbmc.pravaah.internal.validation.SchemaValidationRunner;
import io.github.beingmartinbmc.pravaah.schema.*;
import io.github.beingmartinbmc.pravaah.xls.XlsReader;
import io.github.beingmartinbmc.pravaah.xlsx.Workbook;
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
            case XLS:
                return new PravaahPipeline(() -> XlsReader.readAll(filePath, options));
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
            case XLS:
                return new PravaahPipeline(() -> XlsReader.readAll(data, options));
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

    public static <T> List<T> read(String filePath, Class<T> type) throws IOException {
        return read(filePath, type, ReadOptions.defaults());
    }

    public static <T> List<T> read(String filePath, Class<T> type, ReadOptions options) throws IOException {
        return PojoMapper.mapRows(read(filePath, options).collect(), type);
    }

    public static <T> List<T> read(byte[] data, Class<T> type, ReadOptions options) throws IOException {
        return PojoMapper.mapRows(read(data, options).collect(), type);
    }

    public static Map<String, List<Row>> readAllSheets(String filePath) throws IOException {
        return readAllSheets(filePath, ReadOptions.defaults());
    }

    public static Map<String, List<Row>> readAllSheets(String filePath, ReadOptions options) throws IOException {
        PravaahFormat format = options.getFormat() != null ? options.getFormat() : PravaahFormat.fromExtension(filePath);
        switch (format) {
            case XLS:
                return workbookToMap(XlsReader.readWorkbook(filePath, options));
            case XLSX:
                return workbookToMap(XlsxReader.readWorkbook(filePath, options));
            default:
                return Collections.singletonMap("Sheet1", read(filePath, options).collect());
        }
    }

    public static Map<String, List<Row>> readAllSheets(byte[] data, ReadOptions options) throws IOException {
        PravaahFormat format = options.getFormat() != null ? options.getFormat() : PravaahFormat.XLSX;
        switch (format) {
            case XLS:
                return workbookToMap(XlsReader.readWorkbook(data, options));
            case XLSX:
                return workbookToMap(XlsxReader.readWorkbook(data, options));
            default:
                return Collections.singletonMap("Sheet1", read(data, options).collect());
        }
    }

    public static ProcessStats stream(String filePath, RowConsumer consumer) throws IOException {
        return stream(filePath, ReadOptions.defaults(), consumer);
    }

    public static ProcessStats stream(String filePath, ReadOptions options, RowConsumer consumer) throws IOException {
        PravaahFormat format = options.getFormat() != null ? options.getFormat() : PravaahFormat.fromExtension(filePath);
        ProcessStats stats = PerfUtils.createStats();
        if (format == PravaahFormat.CSV) {
            try (FileInputStream fis = new FileInputStream(filePath)) {
                CsvReader.scanRows(fis, options, (row, rowNumber) -> acceptStreamRow(row, rowNumber, consumer, stats, options));
            }
        } else {
            int rowNumber = 1;
            for (Row row : read(filePath, options).collect()) {
                acceptStreamRow(row, rowNumber++, consumer, stats, options);
            }
        }
        return PerfUtils.finishStats(stats);
    }

    public static ProcessStats stream(byte[] data, ReadOptions options, RowConsumer consumer) throws IOException {
        PravaahFormat format = options.getFormat() != null ? options.getFormat() : PravaahFormat.XLSX;
        ProcessStats stats = PerfUtils.createStats();
        if (format == PravaahFormat.CSV) {
            CsvReader.scanRows(new ByteArrayInputStream(data), options,
                    (row, rowNumber) -> acceptStreamRow(row, rowNumber, consumer, stats, options));
        } else {
            int rowNumber = 1;
            for (Row row : read(data, options).collect()) {
                acceptStreamRow(row, rowNumber++, consumer, stats, options);
            }
        }
        return PerfUtils.finishStats(stats);
    }

    public static ProcessStats stream(String filePath, SchemaDefinition definition,
                                      ReadOptions options, RowConsumer consumer) throws IOException {
        PravaahFormat format = options.getFormat() != null ? options.getFormat() : PravaahFormat.fromExtension(filePath);
        ProcessStats stats = PerfUtils.createStats();
        StreamingValidator validator = new StreamingValidator(definition, options, consumer, stats);
        if (format == PravaahFormat.CSV) {
            try (FileInputStream fis = new FileInputStream(filePath)) {
                CsvReader.scanRows(fis, options, validator::accept);
            }
        } else {
            int rowNumber = 1;
            for (Row row : read(filePath, options).collect()) {
                validator.accept(row, rowNumber++);
            }
        }
        return PerfUtils.finishStats(stats);
    }

    public static ProcessStats stream(byte[] data, SchemaDefinition definition,
                                      ReadOptions options, RowConsumer consumer) throws IOException {
        PravaahFormat format = options.getFormat() != null ? options.getFormat() : PravaahFormat.XLSX;
        ProcessStats stats = PerfUtils.createStats();
        StreamingValidator validator = new StreamingValidator(definition, options, consumer, stats);
        if (format == PravaahFormat.CSV) {
            CsvReader.scanRows(new ByteArrayInputStream(data), options, validator::accept);
        } else {
            int rowNumber = 1;
            for (Row row : read(data, options).collect()) {
                validator.accept(row, rowNumber++);
            }
        }
        return PerfUtils.finishStats(stats);
    }

    // --- Write ---

    public static ProcessStats write(List<Row> rows, String destination) throws IOException {
        return write(rows, destination, WriteOptions.defaults());
    }

    public static ProcessStats write(List<Row> rows, String destination, WriteOptions options) throws IOException {
        rows = prepareRowsForWrite(rows, options);
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

    public static ProcessStats write(List<Row> rows, OutputStream output, WriteOptions options) throws IOException {
        rows = prepareRowsForWrite(rows, options);
        ProcessStats stats = PerfUtils.createStats();
        stats.setRowsProcessed(rows.size());
        stats.setRowsWritten(rows.size());

        PravaahFormat format = options.getFormat() != null ? options.getFormat() : PravaahFormat.CSV;
        switch (format) {
            case CSV:
                CsvWriter.write(rows, output, options);
                break;
            case XLSX:
                XlsxWriter.writeRows(rows, output, options);
                break;
            case JSON:
                JsonRowWriter.writeRows(rows, output);
                break;
            default:
                throw new IllegalArgumentException("Unsupported write format for OutputStream: " + format);
        }

        return PerfUtils.finishStats(stats);
    }

    // --- Parse ---

    public static List<Row> parse(byte[] data, SchemaDefinition definition, ReadOptions options) throws IOException {
        return parseDetailed(data, definition, options).getRows();
    }

    public static List<Row> parse(String filePath, SchemaDefinition definition, ReadOptions options) throws IOException {
        return parseDetailed(filePath, definition, options).getRows();
    }

    // --- Parse Detailed ---

    public static ProcessResult parseDetailed(byte[] data, SchemaDefinition definition, ReadOptions options) throws IOException {
        if (options.getFormat() == PravaahFormat.CSV) {
            return parseDetailedCsv(new ByteArrayInputStream(data), definition, options);
        }
        List<Row> raw = read(data, options).collect();
        return parseDetailed(raw, definition, options);
    }

    public static ProcessResult parseDetailed(String filePath, SchemaDefinition definition, ReadOptions options) throws IOException {
        PravaahFormat format = options.getFormat() != null ? options.getFormat() : PravaahFormat.fromExtension(filePath);
        if (format == PravaahFormat.CSV) {
            try (FileInputStream fis = new FileInputStream(filePath)) {
                return parseDetailedCsv(fis, definition, options);
            }
        }
        List<Row> raw = read(filePath, options).collect();
        return parseDetailed(raw, definition, options);
    }

    private static ProcessResult parseDetailedCsv(InputStream stream, SchemaDefinition definition, ReadOptions options) throws IOException {
        ValidationMode mode = options.getValidation() != null ? options.getValidation() : ValidationMode.COLLECT;
        SchemaValidationRunner runner = new SchemaValidationRunner(definition, mode, options.getCleaning(),
                options.getProgressConsumer(), options.getIssueConsumer(), options.isStrictHeaders());
        CsvReader.scanRows(stream, options, runner::accept);
        return runner.finish();
    }

    private static ProcessResult parseDetailed(List<Row> raw, SchemaDefinition definition, ReadOptions options) {
        ProcessStats stats = PerfUtils.createStats();
        List<Row> validRows = new ArrayList<>(raw.size());
        List<PravaahIssue> issues = new ArrayList<>();
        int rowNumber = 1;
        Set<String> seen = new HashSet<>();

        ValidationMode mode = options.getValidation() != null ? options.getValidation() : ValidationMode.COLLECT;

        if (options.isStrictHeaders() && !raw.isEmpty()) {
            List<PravaahIssue> headerIssues = SchemaValidator.validateHeaders(raw.get(0).keySet(), definition, options.getCleaning());
            if (!headerIssues.isEmpty()) {
                issues.addAll(headerIssues);
                stats.addErrors(headerIssues.size());
                if (options.getIssueConsumer() != null) {
                    for (PravaahIssue issue : headerIssues) options.getIssueConsumer().accept(issue);
                }
                if (mode == ValidationMode.FAIL_FAST) {
                    throw new PravaahValidationException(headerIssues);
                }
            }
        }

        for (Row row : raw) {
            Row cleaned = SchemaValidator.cleanRow(row, options.getCleaning());
            if (options.getCleaning() != null && options.getCleaning().isDropBlankRows()
                    && SchemaValidator.isBlankRow(cleaned)) {
                rowNumber++;
                continue;
            }
            if (options.getCleaning() != null && SchemaValidator.isDuplicate(cleaned,
                    options.getCleaning().getDedupeKey(), seen)) {
                rowNumber++;
                continue;
            }
            SchemaValidator.ValidationResult result = SchemaValidator.validateRow(cleaned, definition, rowNumber);
            stats.incrementRowsProcessed();
            if (options.getProgressConsumer() != null) options.getProgressConsumer().accept(stats.getRowsProcessed());

            if (result.getValue() != null) {
                validRows.add(result.getValue());
            } else if (mode != ValidationMode.SKIP) {
                issues.addAll(result.getIssues());
                stats.addErrors(result.getIssues().size());
                if (options.getIssueConsumer() != null) {
                    for (PravaahIssue issue : result.getIssues()) options.getIssueConsumer().accept(issue);
                }
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

    private static Map<String, List<Row>> workbookToMap(Workbook workbook) {
        Map<String, List<Row>> sheets = new LinkedHashMap<>();
        for (io.github.beingmartinbmc.pravaah.xlsx.Worksheet sheet : workbook.getSheets()) {
            sheets.put(sheet.getName(), sheet.getRows());
        }
        return sheets;
    }

    private static void acceptStreamRow(Row row, int rowNumber, RowConsumer consumer,
                                        ProcessStats stats, ReadOptions options) {
        stats.incrementRowsProcessed();
        if (options.getProgressConsumer() != null) options.getProgressConsumer().accept(stats.getRowsProcessed());
        if ((rowNumber & 4095) == 0) PerfUtils.observeMemory(stats);
        consumer.accept(row, rowNumber);
    }

    private static List<Row> prepareRowsForWrite(List<Row> rows, WriteOptions options) {
        if (options.getSchema() == null) return rows;
        ValidationMode mode = options.getValidation() != null ? options.getValidation() : ValidationMode.FAIL_FAST;
        ProcessResult result = SchemaValidator.validateRows(rows, options.getSchema(), mode, options.getCleaning());
        if (!result.getIssues().isEmpty() && mode == ValidationMode.FAIL_FAST) {
            throw new PravaahValidationException(result.getIssues());
        }
        return result.getRows();
    }

    private static class StreamingValidator {
        private final SchemaDefinition definition;
        private final ReadOptions options;
        private final RowConsumer consumer;
        private final ProcessStats stats;
        private final Set<String> seen = new HashSet<>();
        private final ValidationMode mode;
        private boolean headersChecked;

        StreamingValidator(SchemaDefinition definition, ReadOptions options, RowConsumer consumer, ProcessStats stats) {
            this.definition = definition;
            this.options = options;
            this.consumer = consumer;
            this.stats = stats;
            this.mode = options.getValidation() != null ? options.getValidation() : ValidationMode.COLLECT;
        }

        void accept(Row row, int rowNumber) {
            if (options.isStrictHeaders() && !headersChecked) {
                headersChecked = true;
                List<PravaahIssue> headerIssues = SchemaValidator.validateHeaders(row.keySet(), definition, options.getCleaning());
                if (!headerIssues.isEmpty()) {
                    stats.addErrors(headerIssues.size());
                    if (options.getIssueConsumer() != null) {
                        for (PravaahIssue issue : headerIssues) options.getIssueConsumer().accept(issue);
                    }
                    if (mode == ValidationMode.FAIL_FAST) throw new PravaahValidationException(headerIssues);
                }
            }

            Row cleaned = SchemaValidator.cleanRow(row, options.getCleaning());
            if (options.getCleaning() != null && options.getCleaning().isDropBlankRows()
                    && SchemaValidator.isBlankRow(cleaned)) {
                return;
            }
            if (options.getCleaning() != null && SchemaValidator.isDuplicate(cleaned,
                    options.getCleaning().getDedupeKey(), seen)) {
                return;
            }

            SchemaValidator.ValidationResult result = SchemaValidator.validateRow(cleaned, definition, rowNumber);
            stats.incrementRowsProcessed();
            if (options.getProgressConsumer() != null) options.getProgressConsumer().accept(stats.getRowsProcessed());
            if ((rowNumber & 4095) == 0) PerfUtils.observeMemory(stats);

            if (result.getValue() != null) {
                consumer.accept(result.getValue(), rowNumber);
            } else if (mode != ValidationMode.SKIP) {
                stats.addErrors(result.getIssues().size());
                if (options.getIssueConsumer() != null) {
                    for (PravaahIssue issue : result.getIssues()) options.getIssueConsumer().accept(issue);
                }
                if (mode == ValidationMode.FAIL_FAST) {
                    throw new PravaahValidationException(result.getIssues());
                }
            }
        }
    }

    // --- JSON helpers ---

    private static List<Row> readJsonFile(String path) throws IOException {
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(new FileInputStream(path), StandardCharsets.UTF_8))) {
            StringBuilder sb = new StringBuilder();
            char[] buf = new char[IOUtils.CHAR_READ_BUFFER_SIZE];
            int read;
            while ((read = reader.read(buf)) != -1) sb.append(buf, 0, read);
            return parseJsonRows(sb.toString());
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
                String val = unescapeJsonString(json.substring(i + 1, valEnd));
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

    private static String unescapeJsonString(String s) {
        int idx = s.indexOf('\\');
        if (idx < 0) return s;
        StringBuilder sb = new StringBuilder(s.length());
        int len = s.length();
        for (int i = 0; i < len; i++) {
            char c = s.charAt(i);
            if (c == '\\' && i + 1 < len) {
                char next = s.charAt(++i);
                switch (next) {
                    case '"':  sb.append('"');  break;
                    case '\\': sb.append('\\'); break;
                    case 'n':  sb.append('\n'); break;
                    case 'r':  sb.append('\r'); break;
                    case 't':  sb.append('\t'); break;
                    case 'b':  sb.append('\b'); break;
                    case 'f':  sb.append('\f'); break;
                    case 'u':
                        if (i + 4 < len) {
                            sb.append((char) Integer.parseInt(s.substring(i + 1, i + 5), 16));
                            i += 4;
                        } else {
                            sb.append('\\').append('u');
                        }
                        break;
                    default:
                        sb.append('\\').append(next);
                }
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    private static void writeJsonFile(List<Row> rows, String destination) throws IOException {
        JsonRowWriter.writeRowsToFile(rows, destination);
    }
}
