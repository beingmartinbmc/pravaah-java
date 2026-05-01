package io.github.beingmartinbmc.pravaah.csv;

import io.github.beingmartinbmc.pravaah.Row;
import io.github.beingmartinbmc.pravaah.WriteOptions;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

public final class CsvWriter {

    private CsvWriter() {}

    public static void write(List<Row> rows, String destination, WriteOptions options) throws IOException {
        String delimiter = options.getDelimiter() != null ? options.getDelimiter() : ",";
        List<String> headers = options.getHeaders();

        if (headers == null && !rows.isEmpty()) {
            headers = new ArrayList<>(rows.get(0).keySet());
        }

        BufferedWriter writer = new BufferedWriter(
                new OutputStreamWriter(new FileOutputStream(destination), StandardCharsets.UTF_8));
        try {
            if (headers != null) {
                for (int i = 0; i < headers.size(); i++) {
                    if (i > 0) writer.write(delimiter);
                    writer.write(csvEscape(headers.get(i)));
                }
                writer.newLine();
            }

            for (Row row : rows) {
                if (headers != null) {
                    for (int i = 0; i < headers.size(); i++) {
                        if (i > 0) writer.write(delimiter);
                        Object val = row.get(headers.get(i));
                        writer.write(csvEscape(val == null ? "" : String.valueOf(val)));
                    }
                } else {
                    boolean first = true;
                    for (Object val : row.values()) {
                        if (!first) writer.write(delimiter);
                        writer.write(csvEscape(val == null ? "" : String.valueOf(val)));
                        first = false;
                    }
                }
                writer.newLine();
            }
        } finally {
            writer.close();
        }
    }

    private static String csvEscape(String value) {
        if (value.contains(",") || value.contains("\"") || value.contains("\n") || value.contains("\r")) {
            return "\"" + value.replace("\"", "\"\"") + "\"";
        }
        return value;
    }
}
