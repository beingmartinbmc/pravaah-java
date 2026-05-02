package io.github.beingmartinbmc.pravaah.csv;

import io.github.beingmartinbmc.pravaah.Row;
import io.github.beingmartinbmc.pravaah.WriteOptions;
import io.github.beingmartinbmc.pravaah.internal.io.IOUtils;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Streaming CSV writer with a manual ASCII fast path. Writes through a
 * 64 KiB byte buffer, escaping quotes, embedded newlines, and the configured
 * delimiter on demand. Falls back to a UTF-8 path only when the value
 * contains non-ASCII characters or the configured delimiter is multi-byte.
 */
public final class CsvWriter {
    private static final int BUFFER_SIZE = IOUtils.LARGE_IO_BUFFER_SIZE;
    private static final byte LF = (byte) '\n';
    private static final byte QUOTE = (byte) '"';
    private static final int MAX_ASCII = 0x7F;
    private static final String DEFAULT_DELIMITER = ",";

    private CsvWriter() {}

    public static void write(List<Row> rows, String destination, WriteOptions options) throws IOException {
        String delimiter = options.getDelimiter() != null ? options.getDelimiter() : DEFAULT_DELIMITER;
        if (delimiter.length() != 1 || delimiter.charAt(0) > MAX_ASCII) {
            writeFallback(rows, destination, options, delimiter);
            return;
        }
        byte delimByte = (byte) delimiter.charAt(0);
        List<String> headers = resolveHeaders(options.getHeaders(), rows);

        try (FileOutputStream fos = new FileOutputStream(destination)) {
            ByteSink sink = new ByteSink(fos, BUFFER_SIZE);
            int headerCount = headers == null ? 0 : headers.size();
            if (headers != null) {
                for (int i = 0; i < headerCount; i++) {
                    if (i > 0) sink.writeByte(delimByte);
                    writeFieldFast(sink, headers.get(i), delimByte);
                }
                sink.writeByte(LF);
            }

            for (Row row : rows) {
                if (headers != null) {
                    for (int i = 0; i < headerCount; i++) {
                        if (i > 0) sink.writeByte(delimByte);
                        Object val = row.get(headers.get(i));
                        if (val == null) continue;
                        writeFieldFast(sink, valueToString(val), delimByte);
                    }
                } else {
                    boolean first = true;
                    for (Object val : row.values()) {
                        if (!first) sink.writeByte(delimByte);
                        if (val != null) writeFieldFast(sink, valueToString(val), delimByte);
                        first = false;
                    }
                }
                sink.writeByte(LF);
            }
            sink.flush();
        }
    }

    private static void writeFallback(List<Row> rows, String destination,
                                      WriteOptions options, String delimiter) throws IOException {
        List<String> headers = resolveHeaders(options.getHeaders(), rows);
        byte[] delimBytes = delimiter.getBytes(StandardCharsets.UTF_8);

        try (FileOutputStream fos = new FileOutputStream(destination)) {
            ByteSink sink = new ByteSink(fos, BUFFER_SIZE);
            if (headers != null) {
                for (int i = 0; i < headers.size(); i++) {
                    if (i > 0) sink.write(delimBytes);
                    writeFieldUtf8(sink, headers.get(i), delimiter);
                }
                sink.writeByte(LF);
            }
            for (Row row : rows) {
                if (headers != null) {
                    for (int i = 0; i < headers.size(); i++) {
                        if (i > 0) sink.write(delimBytes);
                        Object val = row.get(headers.get(i));
                        if (val != null) writeFieldUtf8(sink, valueToString(val), delimiter);
                    }
                } else {
                    boolean first = true;
                    for (Object val : row.values()) {
                        if (!first) sink.write(delimBytes);
                        if (val != null) writeFieldUtf8(sink, valueToString(val), delimiter);
                        first = false;
                    }
                }
                sink.writeByte(LF);
            }
            sink.flush();
        }
    }

    private static List<String> resolveHeaders(List<String> requested, List<Row> rows) {
        if (requested != null) return requested;
        if (rows.isEmpty()) return null;
        return new ArrayList<>(rows.get(0).keySet());
    }

    private static void writeFieldFast(ByteSink sink, String value, byte delimByte) throws IOException {
        int length = value.length();
        if (length == 0) return;

        boolean asciiSafe = true;
        boolean needsQuote = false;
        boolean hasQuote = false;
        for (int i = 0; i < length; i++) {
            char c = value.charAt(i);
            if (c > MAX_ASCII) {
                asciiSafe = false;
                continue;
            }
            byte b = (byte) c;
            if (b == QUOTE) { hasQuote = true; needsQuote = true; }
            else if (b == delimByte || b == '\n' || b == '\r') needsQuote = true;
        }

        if (asciiSafe && !needsQuote) {
            sink.writeAscii(value, 0, length);
            return;
        }

        if (asciiSafe) {
            sink.writeByte(QUOTE);
            if (!hasQuote) {
                sink.writeAscii(value, 0, length);
            } else {
                int start = 0;
                for (int i = 0; i < length; i++) {
                    if (value.charAt(i) == '"') {
                        if (i > start) sink.writeAscii(value, start, i);
                        sink.writeByte(QUOTE);
                        sink.writeByte(QUOTE);
                        start = i + 1;
                    }
                }
                if (start < length) sink.writeAscii(value, start, length);
            }
            sink.writeByte(QUOTE);
            return;
        }

        if (!needsQuote) {
            sink.write(value.getBytes(StandardCharsets.UTF_8));
            return;
        }

        sink.writeByte(QUOTE);
        if (!hasQuote) {
            sink.write(value.getBytes(StandardCharsets.UTF_8));
        } else {
            int start = 0;
            for (int i = 0; i < length; i++) {
                if (value.charAt(i) == '"') {
                    if (i > start) sink.write(value.substring(start, i).getBytes(StandardCharsets.UTF_8));
                    sink.writeByte(QUOTE);
                    sink.writeByte(QUOTE);
                    start = i + 1;
                }
            }
            if (start < length) sink.write(value.substring(start).getBytes(StandardCharsets.UTF_8));
        }
        sink.writeByte(QUOTE);
    }

    private static void writeFieldUtf8(ByteSink sink, String value, String delimiter) throws IOException {
        if (value.isEmpty()) return;
        boolean needsQuote = value.indexOf('"') >= 0
                || value.indexOf('\n') >= 0
                || value.indexOf('\r') >= 0
                || value.contains(delimiter);
        if (!needsQuote) {
            sink.write(value.getBytes(StandardCharsets.UTF_8));
            return;
        }
        sink.writeByte(QUOTE);
        if (value.indexOf('"') < 0) {
            sink.write(value.getBytes(StandardCharsets.UTF_8));
        } else {
            sink.write(value.replace("\"", "\"\"").getBytes(StandardCharsets.UTF_8));
        }
        sink.writeByte(QUOTE);
    }

    private static String valueToString(Object val) {
        if (val instanceof String) return (String) val;
        return String.valueOf(val);
    }

    /** Minimal write-only buffer that avoids per-write virtual dispatch overhead. */
    private static final class ByteSink {
        private final OutputStream out;
        private final byte[] buffer;
        private int pos;

        ByteSink(OutputStream out, int capacity) {
            this.out = out;
            this.buffer = new byte[capacity];
        }

        void writeByte(byte b) throws IOException {
            if (pos == buffer.length) drain();
            buffer[pos++] = b;
        }

        void write(byte[] bytes) throws IOException {
            int len = bytes.length;
            if (len > buffer.length - pos) {
                drain();
                if (len > buffer.length) {
                    out.write(bytes);
                    return;
                }
            }
            System.arraycopy(bytes, 0, buffer, pos, len);
            pos += len;
        }

        void writeAscii(String value, int start, int end) throws IOException {
            int len = end - start;
            if (len <= 0) return;
            int remaining = len;
            int srcPos = start;
            while (remaining > 0) {
                int free = buffer.length - pos;
                if (free == 0) {
                    drain();
                    free = buffer.length;
                }
                int chunk = Math.min(free, remaining);
                int chunkEnd = srcPos + chunk;
                for (int i = srcPos; i < chunkEnd; i++) {
                    buffer[pos++] = (byte) value.charAt(i);
                }
                srcPos = chunkEnd;
                remaining -= chunk;
            }
        }

        void flush() throws IOException {
            drain();
            out.flush();
        }

        private void drain() throws IOException {
            if (pos > 0) {
                out.write(buffer, 0, pos);
                pos = 0;
            }
        }
    }
}
