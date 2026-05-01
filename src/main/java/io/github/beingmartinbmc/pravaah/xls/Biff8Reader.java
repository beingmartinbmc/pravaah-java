package io.github.beingmartinbmc.pravaah.xls;

import io.github.beingmartinbmc.pravaah.ReadOptions;
import io.github.beingmartinbmc.pravaah.Row;
import io.github.beingmartinbmc.pravaah.xlsx.FormulaCell;
import io.github.beingmartinbmc.pravaah.xlsx.Workbook;
import io.github.beingmartinbmc.pravaah.xlsx.Worksheet;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

final class Biff8Reader {
    private static final int CONTINUE = 0x003C;
    private static final int EOF = 0x000A;
    private static final int BOUNDSHEET = 0x0085;
    private static final int SST = 0x00FC;
    private static final int LABEL = 0x0204;
    private static final int LABELSST = 0x00FD;
    private static final int NUMBER = 0x0203;
    private static final int RK = 0x027E;
    private static final int MULRK = 0x00BD;
    private static final int BLANK = 0x0201;
    private static final int MULBLANK = 0x00BE;
    private static final int BOOLERR = 0x0205;
    private static final int FORMULA = 0x0006;
    private static final int STRING = 0x0207;

    private final byte[] workbookStream;
    private final List<SheetEntry> sheets;
    private final List<String> sharedStrings;

    Biff8Reader(byte[] workbookStream) throws IOException {
        this.workbookStream = workbookStream;
        this.sheets = readSheetEntries(workbookStream);
        if (sheets.isEmpty()) {
            throw new IOException("No worksheets found in XLS workbook");
        }
        this.sharedStrings = readSharedStrings(workbookStream);
    }

    List<Row> readAll(ReadOptions options) {
        SheetEntry target = findTarget(options);
        return materialize(parseSheet(target), options);
    }

    Workbook readWorkbook(ReadOptions options) {
        List<Worksheet> worksheets = new ArrayList<>();
        for (SheetEntry sheet : sheets) {
            worksheets.add(new Worksheet(sheet.name, materialize(parseSheet(sheet), options)));
        }
        return new Workbook(worksheets);
    }

    private List<Object[]> parseSheet(SheetEntry sheet) {
        TreeMap<Integer, TreeMap<Integer, Object>> rows = new TreeMap<>();
        PendingFormulaString pendingFormulaString = null;
        int cursor = sheet.offset;

        while (cursor + 4 <= workbookStream.length) {
            int sid = u16(workbookStream, cursor);
            int len = u16(workbookStream, cursor + 2);
            int payload = cursor + 4;
            if (payload + len > workbookStream.length) break;

            if (sid == EOF) break;

            switch (sid) {
                case LABELSST: {
                    int row = u16(workbookStream, payload);
                    int col = u16(workbookStream, payload + 2);
                    int sstIndex = i32(workbookStream, payload + 6);
                    set(rows, row, col, sstIndex >= 0 && sstIndex < sharedStrings.size() ? sharedStrings.get(sstIndex) : "");
                    break;
                }
                case LABEL: {
                    int row = u16(workbookStream, payload);
                    int col = u16(workbookStream, payload + 2);
                    BiffString text = readShortString(workbookStream, payload + 6, payload + len);
                    set(rows, row, col, text.value);
                    break;
                }
                case NUMBER: {
                    int row = u16(workbookStream, payload);
                    int col = u16(workbookStream, payload + 2);
                    set(rows, row, col, normalizeNumber(Double.longBitsToDouble(i64(workbookStream, payload + 6))));
                    break;
                }
                case RK: {
                    int row = u16(workbookStream, payload);
                    int col = u16(workbookStream, payload + 2);
                    set(rows, row, col, normalizeNumber(decodeRk(i32(workbookStream, payload + 6))));
                    break;
                }
                case MULRK: {
                    int row = u16(workbookStream, payload);
                    int firstCol = u16(workbookStream, payload + 2);
                    int lastCol = u16(workbookStream, payload + len - 2);
                    int p = payload + 4;
                    for (int col = firstCol; col <= lastCol && p + 6 <= payload + len - 2; col++) {
                        set(rows, row, col, normalizeNumber(decodeRk(i32(workbookStream, p + 2))));
                        p += 6;
                    }
                    break;
                }
                case BLANK: {
                    set(rows, u16(workbookStream, payload), u16(workbookStream, payload + 2), null);
                    break;
                }
                case MULBLANK: {
                    int row = u16(workbookStream, payload);
                    int firstCol = u16(workbookStream, payload + 2);
                    int lastCol = u16(workbookStream, payload + len - 2);
                    for (int col = firstCol; col <= lastCol; col++) set(rows, row, col, null);
                    break;
                }
                case BOOLERR: {
                    int row = u16(workbookStream, payload);
                    int col = u16(workbookStream, payload + 2);
                    int value = workbookStream[payload + 6] & 0xFF;
                    boolean isError = (workbookStream[payload + 7] & 0xFF) != 0;
                    set(rows, row, col, isError ? null : value != 0);
                    break;
                }
                case FORMULA: {
                    int row = u16(workbookStream, payload);
                    int col = u16(workbookStream, payload + 2);
                    Object cached = decodeFormulaResult(workbookStream, payload + 6);
                    if (cached == FormulaStringMarker.INSTANCE) {
                        pendingFormulaString = new PendingFormulaString(row, col);
                    } else {
                        set(rows, row, col, cached);
                        pendingFormulaString = null;
                    }
                    break;
                }
                case STRING: {
                    if (pendingFormulaString != null) {
                        BiffString text = readShortString(workbookStream, payload, payload + len);
                        set(rows, pendingFormulaString.row, pendingFormulaString.col, text.value);
                        pendingFormulaString = null;
                    }
                    break;
                }
                default:
                    break;
            }

            cursor = payload + len;
        }

        List<Object[]> out = new ArrayList<>();
        for (Map.Entry<Integer, TreeMap<Integer, Object>> entry : rows.entrySet()) {
            TreeMap<Integer, Object> cells = entry.getValue();
            int maxCol = cells.isEmpty() ? -1 : cells.lastKey();
            Object[] values = new Object[maxCol + 1];
            for (Map.Entry<Integer, Object> cell : cells.entrySet()) {
                values[cell.getKey()] = cell.getValue();
            }
            out.add(values);
        }
        return out;
    }

    private List<Row> materialize(List<Object[]> valueRows, ReadOptions options) {
        boolean useExplicitHeaders = options.getHeaderNames() != null && !options.getHeaderNames().isEmpty();
        boolean headerless = !useExplicitHeaders && options.getHeaders() != null && !options.getHeaders();
        String[] headers = useExplicitHeaders ? options.getHeaderNames().toArray(new String[0]) : null;
        List<Row> result = new ArrayList<>();

        for (Object[] values : valueRows) {
            if (headers == null && !headerless) {
                headers = new String[values.length];
                for (int i = 0; i < values.length; i++) {
                    headers[i] = values[i] != null ? String.valueOf(values[i]) : ("_" + (i + 1));
                }
                continue;
            }

            Row row = new Row();
            if (headerless) {
                for (int i = 0; i < values.length; i++) {
                    row.put("_" + (i + 1), values[i]);
                }
            } else if (headers != null) {
                for (int i = 0; i < headers.length; i++) {
                    row.put(headers[i], i < values.length ? values[i] : null);
                }
            }
            result.add(row);
        }
        return result;
    }

    private SheetEntry findTarget(ReadOptions options) {
        if (options.getSheetName() != null) {
            for (SheetEntry sheet : sheets) {
                if (sheet.name.equals(options.getSheetName())) return sheet;
            }
            throw new IllegalArgumentException("Worksheet not found: " + options.getSheetName());
        }
        int index = options.getSheetIndex() != null ? options.getSheetIndex() : 0;
        if (index < 0 || index >= sheets.size()) {
            throw new IllegalArgumentException("Worksheet not found: " + index);
        }
        return sheets.get(index);
    }

    private static List<SheetEntry> readSheetEntries(byte[] bytes) {
        List<SheetEntry> entries = new ArrayList<>();
        for (Record record : records(bytes, 0)) {
            if (record.sid == BOUNDSHEET && record.length >= 8) {
                int offset = i32(bytes, record.offset);
                int nameOffset = record.offset + 6;
                BiffString name = readByteCountString(bytes, nameOffset, record.offset + record.length);
                entries.add(new SheetEntry(name.value, offset));
            }
            if (record.sid == EOF && !entries.isEmpty()) break;
        }
        return entries;
    }

    private static List<String> readSharedStrings(byte[] bytes) {
        List<Record> all = records(bytes, 0);
        for (int i = 0; i < all.size(); i++) {
            Record record = all.get(i);
            if (record.sid != SST) continue;

            List<byte[]> segments = new ArrayList<>();
            segments.add(Arrays.copyOfRange(bytes, record.offset, record.offset + record.length));
            int j = i + 1;
            while (j < all.size() && all.get(j).sid == CONTINUE) {
                Record cont = all.get(j);
                segments.add(Arrays.copyOfRange(bytes, cont.offset, cont.offset + cont.length));
                j++;
            }
            return parseSst(segments);
        }
        return new ArrayList<>();
    }

    private static List<String> parseSst(List<byte[]> segments) {
        ContinuationReader reader = new ContinuationReader(segments);
        if (reader.remaining() < 8) return new ArrayList<>();
        reader.readInt();
        int uniqueCount = reader.readInt();
        List<String> strings = new ArrayList<>(Math.max(0, uniqueCount));
        for (int i = 0; i < uniqueCount && reader.remaining() > 0; i++) {
            strings.add(reader.readUnicodeString());
        }
        return strings;
    }

    private static List<Record> records(byte[] bytes, int start) {
        List<Record> out = new ArrayList<>();
        int cursor = Math.max(0, start);
        while (cursor + 4 <= bytes.length) {
            int sid = u16(bytes, cursor);
            int len = u16(bytes, cursor + 2);
            int payload = cursor + 4;
            if (payload + len > bytes.length) break;
            out.add(new Record(sid, payload, len));
            cursor = payload + len;
        }
        return out;
    }

    private static BiffString readByteCountString(byte[] bytes, int offset, int limit) {
        if (offset >= limit) return new BiffString("", offset);
        int cch = bytes[offset] & 0xFF;
        return readUnicodeString(bytes, offset + 1, limit, cch);
    }

    private static BiffString readShortString(byte[] bytes, int offset, int limit) {
        if (offset + 2 > limit) return new BiffString("", limit);
        int cch = u16(bytes, offset);
        return readUnicodeString(bytes, offset + 2, limit, cch);
    }

    private static BiffString readUnicodeString(byte[] bytes, int offset, int limit, int cch) {
        if (offset >= limit) return new BiffString("", offset);
        int flags = bytes[offset++] & 0xFF;
        boolean unicode = (flags & 0x01) != 0;
        boolean rich = (flags & 0x08) != 0;
        boolean extended = (flags & 0x04) != 0;
        int richRuns = 0;
        int extensionSize = 0;
        if (rich && offset + 2 <= limit) {
            richRuns = u16(bytes, offset);
            offset += 2;
        }
        if (extended && offset + 4 <= limit) {
            extensionSize = i32(bytes, offset);
            offset += 4;
        }

        StringBuilder sb = new StringBuilder(cch);
        for (int i = 0; i < cch && offset < limit; i++) {
            if (unicode) {
                if (offset + 2 > limit) break;
                sb.append((char) u16(bytes, offset));
                offset += 2;
            } else {
                sb.append((char) (bytes[offset++] & 0xFF));
            }
        }
        offset = Math.min(limit, offset + richRuns * 4 + extensionSize);
        return new BiffString(sb.toString(), offset);
    }

    private static Object decodeFormulaResult(byte[] bytes, int offset) {
        if ((bytes[offset + 6] & 0xFF) == 0xFF && (bytes[offset + 7] & 0xFF) == 0xFF) {
            int type = bytes[offset] & 0xFF;
            if (type == 0) return FormulaStringMarker.INSTANCE;
            if (type == 1) return (bytes[offset + 2] & 0xFF) != 0;
            return null;
        }
        return normalizeNumber(Double.longBitsToDouble(i64(bytes, offset)));
    }

    private static double decodeRk(int raw) {
        boolean divideBy100 = (raw & 0x01) != 0;
        boolean integer = (raw & 0x02) != 0;
        double value;
        if (integer) {
            value = raw >> 2;
        } else {
            long bits = (raw & 0xFFFFFFFCL) << 32;
            value = Double.longBitsToDouble(bits);
        }
        return divideBy100 ? value / 100.0d : value;
    }

    private static Object normalizeNumber(double value) {
        if (Double.isNaN(value) || Double.isInfinite(value)) return value;
        if (value == Math.floor(value) && value >= Integer.MIN_VALUE && value <= Integer.MAX_VALUE) {
            return (int) value;
        }
        return value;
    }

    private static void set(TreeMap<Integer, TreeMap<Integer, Object>> rows, int row, int col, Object value) {
        TreeMap<Integer, Object> cells = rows.get(row);
        if (cells == null) {
            cells = new TreeMap<>();
            rows.put(row, cells);
        }
        cells.put(col, value);
    }

    private static int u16(byte[] bytes, int offset) {
        return (bytes[offset] & 0xFF) | ((bytes[offset + 1] & 0xFF) << 8);
    }

    private static int i32(byte[] bytes, int offset) {
        return (bytes[offset] & 0xFF)
                | ((bytes[offset + 1] & 0xFF) << 8)
                | ((bytes[offset + 2] & 0xFF) << 16)
                | (bytes[offset + 3] << 24);
    }

    private static long i64(byte[] bytes, int offset) {
        return (i32(bytes, offset) & 0xFFFFFFFFL) | (((long) i32(bytes, offset + 4)) << 32);
    }

    private static final class Record {
        final int sid;
        final int offset;
        final int length;

        Record(int sid, int offset, int length) {
            this.sid = sid;
            this.offset = offset;
            this.length = length;
        }
    }

    private static final class SheetEntry {
        final String name;
        final int offset;

        SheetEntry(String name, int offset) {
            this.name = name;
            this.offset = offset;
        }
    }

    private static final class BiffString {
        final String value;
        final int nextOffset;

        BiffString(String value, int nextOffset) {
            this.value = value;
            this.nextOffset = nextOffset;
        }
    }

    private static final class PendingFormulaString {
        final int row;
        final int col;

        PendingFormulaString(int row, int col) {
            this.row = row;
            this.col = col;
        }
    }

    private enum FormulaStringMarker {
        INSTANCE
    }

    private static final class ContinuationReader {
        private final List<byte[]> segments;
        private int segmentIndex;
        private int offset;

        ContinuationReader(List<byte[]> segments) {
            this.segments = segments;
        }

        int remaining() {
            int total = 0;
            for (int i = segmentIndex; i < segments.size(); i++) {
                total += segments.get(i).length;
            }
            return total - offset;
        }

        int readUnsignedByte() {
            ensureAvailable(false);
            return segments.get(segmentIndex)[offset++] & 0xFF;
        }

        int readUnsignedShort() {
            int b0 = readUnsignedByte();
            int b1 = readUnsignedByte();
            return b0 | (b1 << 8);
        }

        int readInt() {
            int b0 = readUnsignedByte();
            int b1 = readUnsignedByte();
            int b2 = readUnsignedByte();
            int b3 = readUnsignedByte();
            return b0 | (b1 << 8) | (b2 << 16) | (b3 << 24);
        }

        String readUnicodeString() {
            int cch = readUnsignedShort();
            int flags = readUnsignedByte();
            boolean unicode = (flags & 0x01) != 0;
            boolean rich = (flags & 0x08) != 0;
            boolean extended = (flags & 0x04) != 0;
            int richRuns = rich ? readUnsignedShort() : 0;
            int extensionSize = extended ? readInt() : 0;
            StringBuilder sb = new StringBuilder(cch);
            for (int i = 0; i < cch; i++) {
                if (atSegmentEnd() && hasNextSegment()) {
                    unicode = (readContinueOption() & 0x01) != 0;
                }
                if (unicode) {
                    sb.append((char) readUnsignedShort());
                } else {
                    sb.append((char) readUnsignedByte());
                }
            }
            skip(richRuns * 4 + extensionSize);
            return sb.toString();
        }

        private int readContinueOption() {
            segmentIndex++;
            offset = 0;
            return readUnsignedByte();
        }

        private void skip(int count) {
            for (int i = 0; i < count && remaining() > 0; i++) readUnsignedByte();
        }

        private boolean atSegmentEnd() {
            return segmentIndex < segments.size() && offset >= segments.get(segmentIndex).length;
        }

        private boolean hasNextSegment() {
            return segmentIndex + 1 < segments.size();
        }

        private void ensureAvailable(boolean stringContinue) {
            while (segmentIndex < segments.size() && offset >= segments.get(segmentIndex).length) {
                segmentIndex++;
                offset = 0;
                if (stringContinue && segmentIndex < segments.size()) offset++;
            }
            if (segmentIndex >= segments.size()) throw new IllegalStateException("Unexpected end of BIFF record");
        }
    }
}
