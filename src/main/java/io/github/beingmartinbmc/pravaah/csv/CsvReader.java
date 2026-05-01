package io.github.beingmartinbmc.pravaah.csv;

import io.github.beingmartinbmc.pravaah.ReadOptions;
import io.github.beingmartinbmc.pravaah.Row;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Custom streaming CSV parser with RFC-compliant quoted fields, CRLF handling,
 * and zero-dependency hot path.
 */
public final class CsvReader {

    private static final int QUOTE = '"';
    private static final int CR = '\r';
    private static final int LF = '\n';

    private CsvReader() {}

    public static List<Row> readAll(String filePath, ReadOptions options) throws IOException {
        return readAll(new FileInputStream(filePath), options);
    }

    public static List<Row> readAll(byte[] data, ReadOptions options) throws IOException {
        return readAll(new ByteArrayInputStream(data), options);
    }

    public static List<Row> readAll(InputStream stream, ReadOptions options) throws IOException {
        boolean useHeaders = options.getHeaders() == null || options.getHeaders();
        boolean inferTypes = options.isInferTypes();
        char delimiter = options.getDelimiter().charAt(0);
        List<String> explicitHeaders = options.getHeaderNames();

        if (options.getDelimiter().length() > 1) {
            throw new IllegalArgumentException("delimiter must be a single character");
        }

        int initialCapacity = Math.max(16, stream.available());
        Reader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
        try {
            String text = readFully(reader, initialCapacity);
            return parseText(text, delimiter, useHeaders, explicitHeaders, inferTypes);
        } finally {
            reader.close();
        }
    }

    static List<Row> parseText(String text, char delimiter, boolean autoHeaders,
                                List<String> explicitHeaders, boolean inferTypes) {
        List<Row> rows = new ArrayList<>();
        String[] headers = explicitHeaders != null ? explicitHeaders.toArray(new String[0]) : null;
        boolean needAutoHeaders = explicitHeaders == null && autoHeaders;
        boolean headerless = !autoHeaders && explicitHeaders == null;

        int cursor = 0;
        while (cursor < text.length()) {
            ParseResult result = parseNextRecord(text, cursor, delimiter);
            if (result == null) {
                String[] fields = parseLastRecord(text.substring(cursor), delimiter);
                if (fields != null && !isEmptyRecord(fields)) {
                    if (needAutoHeaders && headers == null) {
                        break;
                    }
                    rows.add(buildRow(fields, headers, headerless, inferTypes));
                }
                break;
            }

            String[] fields = result.fields;
            cursor = result.nextCursor;

            if (isEmptyRecord(fields)) continue;

            if (needAutoHeaders && headers == null) {
                headers = fields;
                continue;
            }

            rows.add(buildRow(fields, headers, headerless, inferTypes));
        }

        return rows;
    }

    public static int drainCount(byte[] data, ReadOptions options) throws IOException {
        return drainCount(new ByteArrayInputStream(data), options);
    }

    public static int drainCount(String filePath, ReadOptions options) throws IOException {
        return drainCount(new FileInputStream(filePath), options);
    }

    public static int drainCount(InputStream stream, ReadOptions options) throws IOException {
        if (options.getDelimiter().length() > 1) {
            throw new IllegalArgumentException("delimiter must be a single character");
        }

        boolean skipFirst = options.getHeaders() == null || options.getHeaders();
        char delimiter = options.getDelimiter().charAt(0);

        int initialCapacity = Math.max(16, stream.available());
        Reader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
        try {
            String text = readFully(reader, initialCapacity);
            return scanRecordCount(text, delimiter, skipFirst);
        } finally {
            reader.close();
        }
    }

    private static int scanRecordCount(String text, char delimiter, boolean skipFirst) {
        boolean inQuotes = false;
        boolean quotePending = false;
        boolean atFieldStart = true;
        boolean recordHasContent = false;
        int records = 0;
        int rows = 0;
        boolean lastWasCR = false;

        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);

            if (lastWasCR) {
                lastWasCR = false;
                if (c == LF) continue;
            }

            if (inQuotes && c == QUOTE) {
                if (quotePending) {
                    quotePending = false;
                    recordHasContent = true;
                } else {
                    quotePending = true;
                }
                continue;
            }

            if (quotePending && c == delimiter) {
                inQuotes = false;
                quotePending = false;
                atFieldStart = true;
                continue;
            }

            if (quotePending && (c == LF || c == CR)) {
                inQuotes = false;
                quotePending = false;
                if (recordHasContent) {
                    records++;
                    if (!(skipFirst && records == 1)) rows++;
                }
                recordHasContent = false;
                atFieldStart = true;
                lastWasCR = (c == CR);
                continue;
            }

            if (quotePending) throw new IllegalStateException("Invalid quoted CSV field");

            if (!inQuotes && (c == LF || c == CR)) {
                if (recordHasContent) {
                    records++;
                    if (!(skipFirst && records == 1)) rows++;
                }
                recordHasContent = false;
                atFieldStart = true;
                lastWasCR = (c == CR);
                continue;
            }

            if (!inQuotes && c == delimiter) {
                atFieldStart = true;
                continue;
            }

            if (!inQuotes && c == QUOTE && atFieldStart) {
                inQuotes = true;
                atFieldStart = false;
                continue;
            }

            recordHasContent = true;
            atFieldStart = false;
        }

        if (inQuotes && !quotePending) throw new IllegalStateException("Unclosed quoted CSV field");
        if (quotePending) {
            inQuotes = false;
            quotePending = false;
        }
        if (recordHasContent) {
            records++;
            if (!(skipFirst && records == 1)) rows++;
        }

        return rows;
    }

    static ParseResult parseNextRecord(String text, int start, char delim) {
        List<String> fields = new ArrayList<>();
        int cursor = start;
        int fieldStart = cursor;
        boolean inQuotes = false;

        while (cursor < text.length()) {
            char c = text.charAt(cursor);

            if (inQuotes) {
                if (c == QUOTE) {
                    if (cursor + 1 < text.length() && text.charAt(cursor + 1) == QUOTE) {
                        cursor += 2;
                        continue;
                    }
                    inQuotes = false;
                    cursor++;
                    continue;
                }
                cursor++;
                continue;
            }

            if (c == QUOTE && cursor == fieldStart) {
                inQuotes = true;
                cursor++;
                continue;
            }

            if (c == delim) {
                fields.add(extractField(text, fieldStart, cursor));
                cursor++;
                fieldStart = cursor;
                continue;
            }

            if (c == CR || c == LF) {
                fields.add(extractField(text, fieldStart, cursor));
                cursor++;
                if (c == CR && cursor < text.length() && text.charAt(cursor) == LF) {
                    cursor++;
                }
                return new ParseResult(fields.toArray(new String[0]), cursor);
            }

            cursor++;
        }

        if (inQuotes) return null;
        return null;
    }

    static String[] parseLastRecord(String text, char delim) {
        List<String> fields = new ArrayList<>();
        int cursor = 0;
        int fieldStart = 0;
        boolean inQuotes = false;

        while (cursor < text.length()) {
            char c = text.charAt(cursor);

            if (inQuotes) {
                if (c == QUOTE) {
                    if (cursor + 1 < text.length() && text.charAt(cursor + 1) == QUOTE) {
                        cursor += 2;
                        continue;
                    }
                    inQuotes = false;
                    cursor++;
                    continue;
                }
                cursor++;
                continue;
            }

            if (c == QUOTE && cursor == fieldStart) {
                inQuotes = true;
                cursor++;
                continue;
            }

            if (c == delim) {
                fields.add(extractField(text, fieldStart, cursor));
                cursor++;
                fieldStart = cursor;
                continue;
            }

            if (c == CR || c == LF) {
                fields.add(extractField(text, fieldStart, cursor));
                cursor++;
                if (c == CR && cursor < text.length() && text.charAt(cursor) == LF) {
                    cursor++;
                }
                fieldStart = cursor;
                continue;
            }

            cursor++;
        }

        if (inQuotes) return null;
        fields.add(extractField(text, fieldStart, cursor));
        return fields.toArray(new String[0]);
    }

    private static String extractField(String text, int start, int end) {
        if (start >= end) return "";
        if (text.charAt(start) == QUOTE && end > start + 1 && text.charAt(end - 1) == QUOTE) {
            return text.substring(start + 1, end - 1).replace("\"\"", "\"");
        }
        return text.substring(start, end);
    }

    private static boolean isEmptyRecord(String[] fields) {
        for (String f : fields) {
            if (!f.isEmpty()) return false;
        }
        return true;
    }

    private static Row buildRow(String[] fields, String[] headers, boolean headerless, boolean inferTypes) {
        int size = headers != null && !headerless ? headers.length : fields.length;
        Row row = new Row(mapCapacity(size));
        if (headerless) {
            for (int i = 0; i < fields.length; i++) {
                String key = "_" + (i + 1);
                row.put(key, inferTypes ? inferValue(fields[i]) : fields[i]);
            }
        } else if (headers != null) {
            for (int i = 0; i < headers.length; i++) {
                String value = i < fields.length ? fields[i] : null;
                row.put(headers[i], inferTypes ? inferValue(value) : value);
            }
        } else {
            for (int i = 0; i < fields.length; i++) {
                row.put("_" + (i + 1), inferTypes ? inferValue(fields[i]) : fields[i]);
            }
        }
        return row;
    }

    private static int mapCapacity(int entries) {
        return Math.max(4, (int) (entries / 0.75f) + 1);
    }

    public static Object inferValue(String value) {
        if (value == null || value.isEmpty()) return null;
        try {
            double d = Double.parseDouble(value);
            if (Double.isFinite(d) && !value.trim().isEmpty()) {
                if (d == Math.floor(d) && !value.contains(".") && d >= Integer.MIN_VALUE && d <= Integer.MAX_VALUE) {
                    return (int) d;
                }
                return d;
            }
        } catch (NumberFormatException ignored) {}
        if ("true".equalsIgnoreCase(value)) return true;
        if ("false".equalsIgnoreCase(value)) return false;
        return value;
    }

    private static String readFully(Reader reader, int initialCapacity) throws IOException {
        StringBuilder sb = new StringBuilder(initialCapacity);
        char[] buffer = new char[8192];
        int read;
        while ((read = reader.read(buffer)) != -1) {
            sb.append(buffer, 0, read);
        }
        return sb.toString();
    }

    static class ParseResult {
        final String[] fields;
        final int nextCursor;

        ParseResult(String[] fields, int nextCursor) {
            this.fields = fields;
            this.nextCursor = nextCursor;
        }
    }
}
