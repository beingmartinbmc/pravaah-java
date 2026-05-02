package io.github.beingmartinbmc.pravaah.csv;

import io.github.beingmartinbmc.pravaah.ReadOptions;
import io.github.beingmartinbmc.pravaah.Row;
import io.github.beingmartinbmc.pravaah.internal.csv.CsvRecordSink;
import io.github.beingmartinbmc.pravaah.internal.csv.CsvRecordScanner;
import io.github.beingmartinbmc.pravaah.internal.csv.DefaultRowMaterializer;
import io.github.beingmartinbmc.pravaah.internal.csv.DirectCsvRecordScanner;
import io.github.beingmartinbmc.pravaah.runtime.RuntimeSupport;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Custom streaming CSV parser with RFC-compliant quoted fields, CRLF handling,
 * and zero-dependency hot path.
 */
public final class CsvReader {

    private static final CsvRecordScanner SCANNER = new DirectCsvRecordScanner();

    private CsvReader() {}

    public static List<Row> readAll(String filePath, ReadOptions options) throws IOException {
        try (FileInputStream fis = new FileInputStream(filePath)) {
            return readAll(fis, options);
        }
    }

    public static List<Row> readAll(byte[] data, ReadOptions options) throws IOException {
        return readAll(new ByteArrayInputStream(data), options);
    }

    public static List<Row> readAll(InputStream stream, ReadOptions options) throws IOException {
        boolean useHeaders = options.getHeaders() == null || options.getHeaders();
        boolean inferTypes = options.isInferTypes();
        String text = readText(stream, options);
        char delimiter = resolveDelimiter(options, text);
        char quote = resolveQuote(options, text);
        List<String> explicitHeaders = options.getHeaderNames();

        return parseText(text, delimiter, quote, useHeaders, explicitHeaders, inferTypes);
    }

    static List<Row> parseText(String text, char delimiter, char quote, boolean autoHeaders,
                                List<String> explicitHeaders, boolean inferTypes) {
        List<Row> rows = new ArrayList<>();
        scanRowsText(text, delimiter, quote, autoHeaders, explicitHeaders, inferTypes,
                (row, rowNumber) -> rows.add(row));
        return rows;
    }

    public static void scanRows(InputStream stream, ReadOptions options, RowConsumer consumer) throws IOException {
        boolean useHeaders = options.getHeaders() == null || options.getHeaders();
        boolean inferTypes = options.isInferTypes();
        boolean needSample = options.isAutoDetectDelimiter() || options.isAutoDetectQuote();
        PushbackReader reader = pushback(reader(stream, options));
        char delimiter;
        char quote;
        if (needSample) {
            String sample = sample(reader);
            delimiter = resolveDelimiter(options, sample);
            quote = resolveQuote(options, sample);
        } else {
            delimiter = singleChar(options.getDelimiter(), "delimiter");
            quote = singleChar(options.getQuote(), "quote");
        }
        scanRowsReader(reader, delimiter, quote, useHeaders, options.getHeaderNames(), inferTypes, consumer);
    }

    private static void scanRowsText(String text, char delimiter, char quote, boolean autoHeaders,
                                     List<String> explicitHeaders, boolean inferTypes,
                                     RowConsumer consumer) {
        String[] headers = explicitHeaders != null ? explicitHeaders.toArray(new String[0]) : null;
        boolean needAutoHeaders = explicitHeaders == null && autoHeaders;
        boolean headerless = !autoHeaders && explicitHeaders == null;
        RowCollectorSink sink = new RowCollectorSink(headers, needAutoHeaders, headerless, inferTypes, consumer);
        SCANNER.scan(text, delimiter, quote, sink);
    }

    public static int drainCount(byte[] data, ReadOptions options) throws IOException {
        return drainCount(new ByteArrayInputStream(data), options);
    }

    public static int drainCount(String filePath, ReadOptions options) throws IOException {
        try (FileInputStream fis = new FileInputStream(filePath)) {
            return drainCount(fis, options);
        }
    }

    public static int drainCount(InputStream stream, ReadOptions options) throws IOException {
        if (options.getDelimiter().length() > 1) {
            throw new IllegalArgumentException("delimiter must be a single character");
        }

        boolean skipFirst = options.getHeaders() == null || options.getHeaders();
        String text = readText(stream, options);
        char delimiter = resolveDelimiter(options, text);
        char quote = resolveQuote(options, text);

        CountSink sink = new CountSink(skipFirst);
        SCANNER.scan(text, delimiter, quote, sink);
        return sink.rows;
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

    public static char detectDelimiter(String sample) {
        char[] candidates = new char[]{',', ';', '\t', '|'};
        int bestScore = -1;
        char best = ',';
        for (char candidate : candidates) {
            int score = firstRecordDelimiterCount(sample, candidate);
            if (score > bestScore) {
                bestScore = score;
                best = candidate;
            }
        }
        return best;
    }

    public static char detectQuote(String sample) {
        int doubleQuotes = count(sample, '"');
        int singleQuotes = count(sample, '\'');
        return singleQuotes > doubleQuotes ? '\'' : '"';
    }

    private static String readText(InputStream stream, ReadOptions options) throws IOException {
        Charset charset = options.getCharset() != null ? options.getCharset() : StandardCharsets.UTF_8;
        if (!options.isAutoDetectEncoding() && StandardCharsets.UTF_8.equals(charset)) {
            int initialCapacity = Math.max(16, stream.available());
            return RuntimeSupport.readUtf8(stream, initialCapacity);
        }
        byte[] bytes = io.github.beingmartinbmc.pravaah.internal.io.IOUtils.readAllBytes(stream);
        DecodePlan plan = decodePlan(bytes, charset, options.isAutoDetectEncoding());
        return new String(bytes, plan.offset, bytes.length - plan.offset, plan.charset);
    }

    private static Reader reader(InputStream stream, ReadOptions options) throws IOException {
        Charset charset = options.getCharset() != null ? options.getCharset() : StandardCharsets.UTF_8;
        if (!options.isAutoDetectEncoding()) {
            return new BufferedReader(new InputStreamReader(stream, charset));
        }
        PushbackInputStream pushback = new PushbackInputStream(stream, 4);
        byte[] prefix = new byte[4];
        int n = pushback.read(prefix);
        if (n > 0) pushback.unread(prefix, 0, n);
        DecodePlan plan = decodePlan(prefix, charset, true);
        for (int i = 0; i < plan.offset; i++) pushback.read();
        return new BufferedReader(new InputStreamReader(pushback, plan.charset));
    }

    private static char resolveDelimiter(ReadOptions options, String sample) {
        if (options.isAutoDetectDelimiter()) {
            return detectDelimiter(sample == null ? "" : sample);
        }
        return singleChar(options.getDelimiter(), "delimiter");
    }

    private static char resolveQuote(ReadOptions options, String sample) {
        if (options.isAutoDetectQuote()) {
            return detectQuote(sample == null ? "" : sample);
        }
        return singleChar(options.getQuote(), "quote");
    }

    private static String sample(PushbackReader reader) throws IOException {
        char[] sample = new char[4096];
        int n = reader.read(sample);
        if (n > 0) {
            reader.unread(sample, 0, n);
            return new String(sample, 0, n);
        }
        return "";
    }

    private static PushbackReader pushback(Reader reader) {
        return reader instanceof PushbackReader ? (PushbackReader) reader : new PushbackReader(reader, 4096);
    }

    private static char singleChar(String value, String name) {
        if (value == null || value.length() != 1) {
            throw new IllegalArgumentException(name + " must be a single character");
        }
        return value.charAt(0);
    }

    private static int firstRecordDelimiterCount(String sample, char delimiter) {
        boolean inQuotes = false;
        char quote = detectQuote(sample);
        int count = 0;
        for (int i = 0; i < sample.length(); i++) {
            char c = sample.charAt(i);
            if (c == quote) {
                if (inQuotes && i + 1 < sample.length() && sample.charAt(i + 1) == quote) {
                    i++;
                } else {
                    inQuotes = !inQuotes;
                }
            } else if (!inQuotes && c == delimiter) {
                count++;
            } else if (!inQuotes && (c == '\n' || c == '\r')) {
                break;
            }
        }
        return count;
    }

    private static int count(String value, char needle) {
        int total = 0;
        for (int i = 0; i < value.length(); i++) if (value.charAt(i) == needle) total++;
        return total;
    }

    private static DecodePlan decodePlan(byte[] bytes, Charset fallback, boolean autoDetect) {
        if (autoDetect && bytes.length >= 3
                && (bytes[0] & 0xFF) == 0xEF && (bytes[1] & 0xFF) == 0xBB && (bytes[2] & 0xFF) == 0xBF) {
            return new DecodePlan(StandardCharsets.UTF_8, 3);
        }
        if (autoDetect && bytes.length >= 2) {
            if ((bytes[0] & 0xFF) == 0xFE && (bytes[1] & 0xFF) == 0xFF) {
                return new DecodePlan(StandardCharsets.UTF_16BE, 2);
            }
            if ((bytes[0] & 0xFF) == 0xFF && (bytes[1] & 0xFF) == 0xFE) {
                return new DecodePlan(StandardCharsets.UTF_16LE, 2);
            }
        }
        return new DecodePlan(fallback, 0);
    }

    private static void scanRowsReader(Reader reader, char delimiter, char quote, boolean autoHeaders,
                                       List<String> explicitHeaders, boolean inferTypes,
                                       RowConsumer consumer) throws IOException {
        String[] headers = explicitHeaders != null ? explicitHeaders.toArray(new String[0]) : null;
        boolean needAutoHeaders = explicitHeaders == null && autoHeaders;
        boolean headerless = !autoHeaders && explicitHeaders == null;
        RowCollectorSink sink = new RowCollectorSink(headers, needAutoHeaders, headerless, inferTypes, consumer);
        PushbackReader pushback = pushback(reader);
        int first;
        while ((first = pushback.read()) != -1) {
            sink.startRecord();
            int fieldIndex = 0;
            int c = first;
            boolean recordDone = false;
            while (!recordDone) {
                if (c == -1) {
                    sink.field(fieldIndex++, "");
                    recordDone = true;
                } else if (c == delimiter) {
                    sink.field(fieldIndex++, "");
                    c = pushback.read();
                    if (c == -1) recordDone = true;
                } else if (c == '\r' || c == '\n') {
                    sink.field(fieldIndex++, "");
                    skipLfAfterCr(pushback, c);
                    recordDone = true;
                } else if (c == quote) {
                    StringBuilder value = new StringBuilder();
                    while (true) {
                        c = pushback.read();
                        if (c == -1) throw new IllegalStateException("Unclosed quoted CSV field");
                        if (c == quote) {
                            int next = pushback.read();
                            if (next == quote) {
                                value.append((char) quote);
                                continue;
                            }
                            sink.field(fieldIndex++, value.toString());
                            if (next == delimiter) {
                                c = pushback.read();
                                if (c == -1) recordDone = true;
                            } else if (next == '\r' || next == '\n' || next == -1) {
                                skipLfAfterCr(pushback, next);
                                recordDone = true;
                            } else {
                                throw new IllegalStateException("Invalid quoted CSV field");
                            }
                            break;
                        }
                        value.append((char) c);
                    }
                } else {
                    StringBuilder value = new StringBuilder();
                    while (c != -1 && c != delimiter && c != '\r' && c != '\n') {
                        value.append((char) c);
                        c = pushback.read();
                    }
                    sink.field(fieldIndex++, value.toString());
                    if (c == delimiter) {
                        c = pushback.read();
                        if (c == -1) recordDone = true;
                    } else {
                        skipLfAfterCr(pushback, c);
                        recordDone = true;
                    }
                }
            }
            sink.endRecord();
        }
    }

    private static void skipLfAfterCr(PushbackReader reader, int c) throws IOException {
        if (c != '\r') return;
        int next = reader.read();
        if (next != '\n' && next != -1) reader.unread(next);
    }

    private static class DecodePlan {
        final Charset charset;
        final int offset;

        DecodePlan(Charset charset, int offset) {
            this.charset = charset;
            this.offset = offset;
        }
    }

    private static class CountSink implements CsvRecordSink {
        private final boolean skipFirst;
        private boolean empty;
        private int records;
        private int rows;

        CountSink(boolean skipFirst) {
            this.skipFirst = skipFirst;
        }

        @Override
        public void startRecord() {
            empty = true;
        }

        @Override
        public void field(int index, String value) {
            if (empty && value != null && !value.isEmpty()) {
                empty = false;
            }
        }

        @Override
        public void field(int index, CharSequence text, int start, int end) {
            if (empty && start != end) {
                empty = false;
            }
        }

        @Override
        public void endRecord() {
            if (empty) return;
            records++;
            if (!(skipFirst && records == 1)) {
                rows++;
            }
        }
    }

    private static class RowCollectorSink implements CsvRecordSink {
        private final boolean needAutoHeaders;
        private final boolean headerless;
        private final boolean inferTypes;
        private final RowConsumer consumer;
        private String[] headers;
        private List<String> headerFields;
        private DefaultRowMaterializer materializer;
        private boolean empty;
        private int rowNumber = 1;

        RowCollectorSink(String[] headers, boolean needAutoHeaders, boolean headerless,
                         boolean inferTypes, RowConsumer consumer) {
            this.headers = headers;
            this.needAutoHeaders = needAutoHeaders;
            this.headerless = headerless;
            this.inferTypes = inferTypes;
            this.consumer = consumer;
            if (!needAutoHeaders || headers != null) {
                this.materializer = new DefaultRowMaterializer(headers, headerless, inferTypes);
            }
        }

        @Override
        public void startRecord() {
            empty = true;
            if (needAutoHeaders && headers == null) {
                headerFields = new ArrayList<>();
            } else {
                materializer.startRecord();
            }
        }

        @Override
        public void field(int index, String value) {
            if (empty && value != null && !value.isEmpty()) {
                empty = false;
            }
            if (headerFields != null) {
                headerFields.add(value);
            } else {
                materializer.field(index, value);
            }
        }

        @Override
        public void field(int index, CharSequence text, int start, int end) {
            if (empty && start != end) {
                empty = false;
            }
            if (headerFields != null) {
                headerFields.add(text.subSequence(start, end).toString());
            } else if (start == end) {
                materializer.fieldEmpty(index);
            } else {
                materializer.field(index, text.subSequence(start, end).toString());
            }
        }

        @Override
        public void endRecord() {
            if (empty) return;
            if (headerFields != null) {
                headers = headerFields.toArray(new String[0]);
                headerFields = null;
                materializer = new DefaultRowMaterializer(headers, headerless, inferTypes);
                return;
            }

            Row row = materializer.finishRecord();
            if (row != null) {
                consumer.accept(row, rowNumber++);
            }
        }
    }
}
