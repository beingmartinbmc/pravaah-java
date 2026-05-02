package io.github.beingmartinbmc.pravaah.diff;

import io.github.beingmartinbmc.pravaah.Row;
import io.github.beingmartinbmc.pravaah.internal.json.JsonRowWriter;
import io.github.beingmartinbmc.pravaah.internal.text.CsvFormat;

import java.io.*;
import java.util.*;

public final class DiffEngine {

    /** Maximum number of leading row values combined into a key preview string. */
    private static final int KEY_PREVIEW_VALUE_LIMIT = 3;

    /** Joins composite key fragments with a NUL byte to avoid ambiguity. */
    private static final char KEY_FRAGMENT_SEPARATOR = '\0';

    private DiffEngine() {}

    public static DiffResult diff(Iterable<Row> oldRows, Iterable<Row> newRows, String... keys) {
        Map<String, Row> oldIndex = indexByKey(oldRows, keys);
        Map<String, Row> newIndex = indexByKey(newRows, keys);

        List<Row> added = new ArrayList<>();
        List<Row> removed = new ArrayList<>();
        List<RowChange> changed = new ArrayList<>();
        int unchanged = 0;

        for (Map.Entry<String, Row> entry : newIndex.entrySet()) {
            Row before = oldIndex.get(entry.getKey());
            Row after = entry.getValue();
            if (before == null) {
                added.add(after);
                continue;
            }
            List<String> changedColumns = changedColumnsFor(before, after);
            if (changedColumns.isEmpty()) {
                unchanged++;
            } else {
                changed.add(new RowChange(entry.getKey(), before, after, changedColumns));
            }
        }

        for (Map.Entry<String, Row> entry : oldIndex.entrySet()) {
            if (!newIndex.containsKey(entry.getKey())) {
                removed.add(entry.getValue());
            }
        }

        return new DiffResult(added, removed, changed, unchanged);
    }

    public static void writeDiffReport(DiffResult result, String destination) throws IOException {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(destination))) {
            writer.write("type,key,changedColumns,before,after");
            writer.newLine();
            for (Row row : result.getAdded()) {
                writer.write(escape("added") + "," + escape(keyPreview(row)) + ",," + "," + escape(rowToJson(row)));
                writer.newLine();
            }
            for (Row row : result.getRemoved()) {
                writer.write(escape("removed") + "," + escape(keyPreview(row)) + ",," + escape(rowToJson(row)) + ",");
                writer.newLine();
            }
            for (RowChange change : result.getChanged()) {
                StringBuilder cols = new StringBuilder();
                for (int i = 0; i < change.getChangedColumns().size(); i++) {
                    if (i > 0) cols.append("|");
                    cols.append(change.getChangedColumns().get(i));
                }
                writer.write(escape("changed") + "," + escape(change.getKey()) + ","
                        + escape(cols.toString()) + "," + escape(rowToJson(change.getBefore()))
                        + "," + escape(rowToJson(change.getAfter())));
                writer.newLine();
            }
        }
    }

    private static Map<String, Row> indexByKey(Iterable<Row> rows, String[] keys) {
        Map<String, Row> index = new LinkedHashMap<>();
        for (Row row : rows) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < keys.length; i++) {
                if (i > 0) sb.append(KEY_FRAGMENT_SEPARATOR);
                Object v = row.get(keys[i]);
                sb.append(v == null ? "" : String.valueOf(v));
            }
            index.put(sb.toString(), row);
        }
        return index;
    }

    private static List<String> changedColumnsFor(Row before, Row after) {
        Set<String> allColumns = new LinkedHashSet<>();
        allColumns.addAll(before.keySet());
        allColumns.addAll(after.keySet());
        List<String> changed = new ArrayList<>();
        for (String col : allColumns) {
            if (!sameValue(before.get(col), after.get(col))) {
                changed.add(col);
            }
        }
        return changed;
    }

    private static boolean sameValue(Object left, Object right) {
        if (left instanceof Date && right instanceof Date) {
            return ((Date) left).getTime() == ((Date) right).getTime();
        }
        return Objects.equals(left, right);
    }

    private static String keyPreview(Row row) {
        StringBuilder sb = new StringBuilder();
        int count = 0;
        for (Object val : row.values()) {
            if (count >= KEY_PREVIEW_VALUE_LIMIT) break;
            if (count > 0) sb.append("|");
            sb.append(val == null ? "" : String.valueOf(val));
            count++;
        }
        return sb.toString();
    }

    private static String rowToJson(Row row) {
        return JsonRowWriter.rowToJson(row);
    }

    private static String escape(String value) {
        return CsvFormat.csvEscape(value);
    }

    public static class DiffResult {
        private final List<Row> added;
        private final List<Row> removed;
        private final List<RowChange> changed;
        private final int unchanged;

        public DiffResult(List<Row> added, List<Row> removed, List<RowChange> changed, int unchanged) {
            this.added = added;
            this.removed = removed;
            this.changed = changed;
            this.unchanged = unchanged;
        }

        public List<Row> getAdded() { return added; }
        public List<Row> getRemoved() { return removed; }
        public List<RowChange> getChanged() { return changed; }
        public int getUnchanged() { return unchanged; }
    }

    public static class RowChange {
        private final String key;
        private final Row before;
        private final Row after;
        private final List<String> changedColumns;

        public RowChange(String key, Row before, Row after, List<String> changedColumns) {
            this.key = key;
            this.before = before;
            this.after = after;
            this.changedColumns = changedColumns;
        }

        public String getKey() { return key; }
        public Row getBefore() { return before; }
        public Row getAfter() { return after; }
        public List<String> getChangedColumns() { return changedColumns; }
    }
}
