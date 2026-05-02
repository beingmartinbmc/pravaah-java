package io.github.beingmartinbmc.pravaah.csv;

import io.github.beingmartinbmc.pravaah.ReadOptions;
import io.github.beingmartinbmc.pravaah.Row;
import io.github.beingmartinbmc.pravaah.internal.csv.CsvRecordSink;
import io.github.beingmartinbmc.pravaah.internal.csv.CsvRecordScanner;
import io.github.beingmartinbmc.pravaah.internal.csv.DefaultRowMaterializer;
import io.github.beingmartinbmc.pravaah.internal.csv.DirectCsvRecordScanner;
import io.github.beingmartinbmc.pravaah.runtime.RuntimeSupport;

import java.io.*;
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
        char delimiter = options.getDelimiter().charAt(0);
        List<String> explicitHeaders = options.getHeaderNames();

        if (options.getDelimiter().length() > 1) {
            throw new IllegalArgumentException("delimiter must be a single character");
        }

        int initialCapacity = Math.max(16, stream.available());
        String text = RuntimeSupport.readUtf8(stream, initialCapacity);
        return parseText(text, delimiter, useHeaders, explicitHeaders, inferTypes);
    }

    static List<Row> parseText(String text, char delimiter, boolean autoHeaders,
                                List<String> explicitHeaders, boolean inferTypes) {
        List<Row> rows = new ArrayList<>();
        scanRowsText(text, delimiter, autoHeaders, explicitHeaders, inferTypes,
                (row, rowNumber) -> rows.add(row));
        return rows;
    }

    public static void scanRows(InputStream stream, ReadOptions options, RowConsumer consumer) throws IOException {
        if (options.getDelimiter().length() > 1) {
            throw new IllegalArgumentException("delimiter must be a single character");
        }

        boolean useHeaders = options.getHeaders() == null || options.getHeaders();
        boolean inferTypes = options.isInferTypes();
        char delimiter = options.getDelimiter().charAt(0);
        int initialCapacity = Math.max(16, stream.available());
        String text = RuntimeSupport.readUtf8(stream, initialCapacity);
        scanRowsText(text, delimiter, useHeaders, options.getHeaderNames(), inferTypes, consumer);
    }

    private static void scanRowsText(String text, char delimiter, boolean autoHeaders,
                                     List<String> explicitHeaders, boolean inferTypes,
                                     RowConsumer consumer) {
        String[] headers = explicitHeaders != null ? explicitHeaders.toArray(new String[0]) : null;
        boolean needAutoHeaders = explicitHeaders == null && autoHeaders;
        boolean headerless = !autoHeaders && explicitHeaders == null;
        RowCollectorSink sink = new RowCollectorSink(headers, needAutoHeaders, headerless, inferTypes, consumer);
        SCANNER.scan(text, delimiter, sink);
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
        char delimiter = options.getDelimiter().charAt(0);

        int initialCapacity = Math.max(16, stream.available());
        String text = RuntimeSupport.readUtf8(stream, initialCapacity);
        CountSink sink = new CountSink(skipFirst);
        SCANNER.scan(text, delimiter, sink);
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
