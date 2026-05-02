package io.github.beingmartinbmc.pravaah.internal.json;

import io.github.beingmartinbmc.pravaah.Row;

import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Internal helper that consolidates the small ad-hoc {@code Row} -> JSON object
 * serializers that previously lived inside {@code Pravaah}, {@code PravaahPipeline},
 * and {@code DiffEngine}. The output is intentionally identical for all callers so
 * round-trips through {@code Pravaah.parseJsonRows(...)} stay consistent.
 */
public final class JsonRowWriter {

    private JsonRowWriter() {}

    /** Appends a JSON object representation of {@code row} to {@code out}. */
    public static void appendRow(StringBuilder out, Row row) {
        out.append('{');
        boolean first = true;
        for (Map.Entry<String, Object> entry : row.entrySet()) {
            if (!first) out.append(", ");
            out.append('"').append(escapeString(entry.getKey())).append("\": ");
            appendValue(out, entry.getValue());
            first = false;
        }
        out.append('}');
    }

    /** Returns the JSON object string representation of {@code row}. */
    public static String rowToJson(Row row) {
        StringBuilder sb = new StringBuilder(64);
        appendRow(sb, row);
        return sb.toString();
    }

    /**
     * Writes {@code rows} as a JSON array to the file at {@code destination}. The output is
     * pretty-printed with each row on its own line so it remains diff-friendly.
     */
    public static void writeRowsToFile(List<Row> rows, String destination) throws IOException {
        StringBuilder sb = new StringBuilder(rows.size() * 64 + 16);
        sb.append("[\n");
        for (int i = 0; i < rows.size(); i++) {
            if (i > 0) sb.append(",\n");
            sb.append("  ");
            appendRow(sb, rows.get(i));
        }
        sb.append("\n]\n");
        try (FileWriter fw = new FileWriter(destination)) {
            fw.write(sb.toString());
        }
    }

    private static void appendValue(StringBuilder out, Object value) {
        if (value == null) {
            out.append("null");
        } else if (value instanceof Number || value instanceof Boolean) {
            out.append(value);
        } else {
            out.append('"').append(escapeString(String.valueOf(value))).append('"');
        }
    }

    private static String escapeString(String value) {
        return value.replace("\"", "\\\"");
    }
}
