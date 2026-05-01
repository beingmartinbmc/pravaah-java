package io.github.beingmartinbmc.pravaah.csv;

import io.github.beingmartinbmc.pravaah.Row;
import io.github.beingmartinbmc.pravaah.WriteOptions;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

public final class CsvWriter {
    private static final int BUFFER_SIZE = 64 * 1024;

    private CsvWriter() {}

    public static void write(List<Row> rows, String destination, WriteOptions options) throws IOException {
        String delimiter = options.getDelimiter() != null ? options.getDelimiter() : ",";
        List<String> headers = options.getHeaders();

        if (headers == null && !rows.isEmpty()) {
            headers = new ArrayList<>(rows.get(0).keySet());
        }

        BufferedWriter writer = new BufferedWriter(
                new OutputStreamWriter(new FileOutputStream(destination), StandardCharsets.UTF_8), BUFFER_SIZE);
        try {
            if (headers != null) {
                for (int i = 0; i < headers.size(); i++) {
                    if (i > 0) writer.write(delimiter);
                    writeEscaped(writer, headers.get(i), delimiter);
                }
                writer.newLine();
            }

            for (Row row : rows) {
                if (headers != null) {
                    for (int i = 0; i < headers.size(); i++) {
                        if (i > 0) writer.write(delimiter);
                        Object val = row.get(headers.get(i));
                        writeEscaped(writer, val == null ? "" : String.valueOf(val), delimiter);
                    }
                } else {
                    boolean first = true;
                    for (Object val : row.values()) {
                        if (!first) writer.write(delimiter);
                        writeEscaped(writer, val == null ? "" : String.valueOf(val), delimiter);
                        first = false;
                    }
                }
                writer.newLine();
            }
        } finally {
            writer.close();
        }
    }

    private static void writeEscaped(Writer writer, String value, String delimiter) throws IOException {
        int quoteIndex = -1;
        boolean escape = false;
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c == '"') {
                quoteIndex = i;
                escape = true;
                break;
            }
            if (c == '\n' || c == '\r') {
                escape = true;
            }
        }
        if (!escape && delimiter != null && !delimiter.isEmpty() && value.indexOf(delimiter) >= 0) {
            escape = true;
        }
        if (!escape) {
            writer.write(value);
            return;
        }

        writer.write('"');
        if (quoteIndex == -1) {
            writer.write(value);
        } else {
            int start = 0;
            for (int i = quoteIndex; i < value.length(); i++) {
                if (value.charAt(i) == '"') {
                    writer.write(value, start, i - start);
                    writer.write("\"\"");
                    start = i + 1;
                }
            }
            writer.write(value, start, value.length() - start);
        }
        writer.write('"');
    }
}
