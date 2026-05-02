package io.github.beingmartinbmc.pravaah.xlsx;

import io.github.beingmartinbmc.pravaah.ReadOptions;
import io.github.beingmartinbmc.pravaah.Row;
import io.github.beingmartinbmc.pravaah.internal.io.IOUtils;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.*;
import java.util.zip.*;

public final class XlsxReader {

    private static final Pattern INLINE_STRING_T = Pattern.compile("<t\\b[^>]*>([\\s\\S]*?)</t>");

    private XlsxReader() {}

    public static List<Row> readAll(String filePath, ReadOptions options) throws IOException {
        return readAll(IOUtils.readAllBytes(filePath), options);
    }

    public static List<Row> readAll(byte[] data, ReadOptions options) throws IOException {
        Map<String, byte[]> files = unzip(data);
        LazySharedStrings sharedStrings = buildSharedStrings(files);
        List<SheetEntry> sheetEntries = resolveSheets(files);

        SheetEntry target = findTarget(sheetEntries, options);
        byte[] sheetData = files.get(target.path);
        if (sheetData == null) {
            throw new IOException("Worksheet XML not found: " + target.path);
        }

        String sheetXml = new String(sheetData, StandardCharsets.UTF_8);
        return parseWorksheetRows(sheetXml, sharedStrings, options);
    }

    public static Workbook readWorkbook(String filePath, ReadOptions options) throws IOException {
        return readWorkbook(IOUtils.readAllBytes(filePath), options);
    }

    public static Workbook readWorkbook(byte[] data, ReadOptions options) throws IOException {
        Map<String, byte[]> files = unzip(data);
        LazySharedStrings sharedStrings = buildSharedStrings(files);
        List<SheetEntry> sheetEntries = resolveSheets(files);

        List<Worksheet> sheets = new ArrayList<>();
        for (SheetEntry entry : sheetEntries) {
            byte[] sheetData = files.get(entry.path);
            if (sheetData == null) throw new IOException("Worksheet XML not found: " + entry.path);
            String sheetXml = new String(sheetData, StandardCharsets.UTF_8);
            List<Row> rows = parseWorksheetRows(sheetXml, sharedStrings, options);
            sheets.add(new Worksheet(entry.name, rows));
        }

        return new Workbook(sheets);
    }

    private static List<Row> parseWorksheetRows(String xml, LazySharedStrings ss, ReadOptions options) {
        boolean useArrayHeaders = options.getHeaderNames() != null && !options.getHeaderNames().isEmpty();
        boolean headerless = options.getHeaders() != null && !options.getHeaders();
        String[] headers = useArrayHeaders ? options.getHeaderNames().toArray(new String[0]) : null;
        boolean firstRowIsHeaders = headers == null && !headerless;

        List<Row> result = new ArrayList<>();

        int sdStart = xml.indexOf("<sheetData");
        if (sdStart == -1) return result;
        int sdOpenEnd = xml.indexOf(">", sdStart);
        int sdEnd = xml.indexOf("</sheetData>", sdOpenEnd);
        if (sdOpenEnd == -1 || sdEnd == -1) return result;

        Integer colCount = parseDimensionColumnCount(xml);

        int cursor = sdOpenEnd + 1;
        while (cursor < sdEnd) {
            int rowStart = xml.indexOf("<row", cursor);
            if (rowStart == -1 || rowStart >= sdEnd) break;

            int rowOpenEnd = xml.indexOf(">", rowStart);
            if (rowOpenEnd == -1) break;

            boolean selfClose = xml.charAt(rowOpenEnd - 1) == '/';
            if (selfClose) {
                cursor = rowOpenEnd + 1;
                if (firstRowIsHeaders && headers == null) {
                    headers = new String[0];
                    firstRowIsHeaders = false;
                    continue;
                }
                result.add(emptyRow(headers, headerless));
                continue;
            }

            int rowEnd = xml.indexOf("</row>", rowOpenEnd);
            if (rowEnd == -1) break;

            Object[] values = parseRowCells(xml, rowOpenEnd + 1, rowEnd, ss, options, colCount);
            cursor = rowEnd + 6;

            if (firstRowIsHeaders && headers == null) {
                headers = new String[values.length];
                for (int i = 0; i < values.length; i++) {
                    headers[i] = values[i] != null ? String.valueOf(values[i]) : ("_" + (i + 1));
                }
                firstRowIsHeaders = false;
                continue;
            }

            result.add(buildRow(values, headers, headerless));
        }

        return result;
    }

    private static Row buildRow(Object[] values, String[] headers, boolean headerless) {
        Row row = new Row(rowMapCapacity(headerless ? values.length : (headers == null ? 0 : headers.length)));
        if (headerless) {
            for (int i = 0; i < values.length; i++) {
                row.put("_" + (i + 1), values[i]);
            }
        } else if (headers != null) {
            int n = headers.length;
            for (int i = 0; i < n; i++) {
                row.put(headers[i], i < values.length ? values[i] : null);
            }
        }
        return row;
    }

    private static Row emptyRow(String[] headers, boolean headerless) {
        if (headerless || headers == null) {
            return new Row();
        }
        Row row = new Row(rowMapCapacity(headers.length));
        for (String h : headers) row.put(h, null);
        return row;
    }

    private static int rowMapCapacity(int size) {
        if (size <= 0) return 16;
        return Math.max(4, (int) (size / 0.75f) + 1);
    }

    private static Integer parseDimensionColumnCount(String xml) {
        int dimIdx = xml.indexOf("<dimension");
        if (dimIdx == -1) return null;
        int dimEnd = xml.indexOf(">", dimIdx);
        if (dimEnd == -1) return null;
        String tag = xml.substring(dimIdx, dimEnd + 1);
        String ref = readXmlAttribute(tag, "ref");
        if (ref == null) return null;
        String[] parts = ref.split(":");
        if (parts.length != 2) return null;
        return cellRefToColumnIndex(parts[1]) + 1;
    }

    private static Object[] parseRowCells(String xml, int rowStart, int rowEnd,
                                           LazySharedStrings ss, ReadOptions options,
                                           Integer colCount) {
        Object[] fixed = colCount != null ? new Object[colCount] : null;
        Object[] dynamic = fixed == null ? new Object[8] : null;
        int dynamicSize = 0;

        int cursor = rowStart;
        int implicitColumn = 0;

        while (cursor < rowEnd) {
            int cellStart = xml.indexOf("<c", cursor);
            if (cellStart == -1 || cellStart >= rowEnd) break;

            int cellOpenEnd = xml.indexOf(">", cellStart);
            if (cellOpenEnd == -1 || cellOpenEnd >= rowEnd) break;

            String ref = readXmlAttributeRange(xml, cellStart, cellOpenEnd + 1, "r");
            String type = readXmlAttributeRange(xml, cellStart, cellOpenEnd + 1, "t");
            int columnIndex = ref != null ? cellRefToColumnIndex(ref) : implicitColumn;
            implicitColumn = columnIndex + 1;

            boolean selfClose = xml.charAt(cellOpenEnd - 1) == '/';

            Object cellValue;
            if (selfClose) {
                cellValue = null;
                cursor = cellOpenEnd + 1;
            } else {
                int cellEnd = xml.indexOf("</c>", cellOpenEnd);
                if (cellEnd == -1 || cellEnd >= rowEnd) break;
                cellValue = decodeCellValueRange(type, xml, cellOpenEnd + 1, cellEnd, ss, options);
                cursor = cellEnd + 4;
            }

            if (fixed != null) {
                if (columnIndex < fixed.length) fixed[columnIndex] = cellValue;
            } else {
                if (columnIndex >= dynamic.length) {
                    int newCap = dynamic.length;
                    while (newCap <= columnIndex) newCap *= 2;
                    Object[] grown = new Object[newCap];
                    System.arraycopy(dynamic, 0, grown, 0, dynamicSize);
                    dynamic = grown;
                }
                dynamic[columnIndex] = cellValue;
                if (columnIndex + 1 > dynamicSize) dynamicSize = columnIndex + 1;
            }
        }

        if (fixed != null) return fixed;
        Object[] trimmed = new Object[dynamicSize];
        System.arraycopy(dynamic, 0, trimmed, 0, dynamicSize);
        return trimmed;
    }

    private static String readXmlAttributeRange(String xml, int tagStart, int tagEnd, String name) {
        int markerLen = name.length() + 1;
        int searchEnd = tagEnd - markerLen;
        for (int i = tagStart; i <= searchEnd; i++) {
            if (xml.charAt(i + markerLen - 1) != '=') continue;
            boolean match = true;
            for (int k = 0; k < name.length(); k++) {
                if (xml.charAt(i + k) != name.charAt(k)) { match = false; break; }
            }
            if (!match) continue;
            char prev = i == tagStart ? ' ' : xml.charAt(i - 1);
            if (prev != ' ' && prev != '\t' && prev != '\n' && prev != '\r' && prev != '<') continue;

            int q = i + markerLen;
            if (q >= tagEnd) return null;
            char quote = xml.charAt(q);
            if (quote != '"' && quote != '\'') return null;
            int end = xml.indexOf(quote, q + 1);
            if (end == -1 || end >= tagEnd) return null;
            return unescapeXml(xml.substring(q + 1, end));
        }
        return null;
    }

    private static Object decodeCellValueRange(String type, String xml, int innerStart, int innerEnd,
                                                LazySharedStrings ss, ReadOptions options) {
        boolean preserveFormula = "preserve".equals(options.getFormulas());

        if (preserveFormula) {
            String formulaText = firstTagTextRange(xml, innerStart, innerEnd, "f");
            if (formulaText != null) {
                String f = formulaText.startsWith("=") ? formulaText.substring(1) : formulaText;
                ReadOptions valuesOpt = new ReadOptions().formulas("values");
                Object result = decodeCellValueRange(type, xml, innerStart, innerEnd, ss, valuesOpt);
                return new FormulaCell(f, result);
            }
        }

        if ("s".equals(type)) {
            String vText = firstTagTextRange(xml, innerStart, innerEnd, "v");
            if (vText == null) return null;
            int idx = Integer.parseInt(vText);
            return ss.get(idx);
        }
        if ("inlineStr".equals(type)) {
            return inlineStringText(xml.substring(innerStart, innerEnd));
        }
        if ("b".equals(type)) {
            return "1".equals(firstTagTextRange(xml, innerStart, innerEnd, "v"));
        }

        String rawValue = firstTagTextRange(xml, innerStart, innerEnd, "v");
        if (rawValue == null) return null;
        try {
            double d = Double.parseDouble(rawValue);
            if (Double.isFinite(d)) {
                if (d == Math.floor(d) && rawValue.indexOf('.') == -1 && d >= Integer.MIN_VALUE && d <= Integer.MAX_VALUE) {
                    return (int) d;
                }
                return d;
            }
        } catch (NumberFormatException ignored) {}
        return rawValue;
    }

    static String readXmlAttribute(String tag, String name) {
        String marker = name + "=";
        int markerIndex = tag.indexOf(marker);
        if (markerIndex == -1) return null;

        int quoteIndex = markerIndex + marker.length();
        if (quoteIndex >= tag.length()) return null;
        char quote = tag.charAt(quoteIndex);
        if (quote != '"' && quote != '\'') return null;

        int valueStart = quoteIndex + 1;
        int valueEnd = tag.indexOf(quote, valueStart);
        return valueEnd == -1 ? null : unescapeXml(tag.substring(valueStart, valueEnd));
    }

    private static String firstTagTextRange(String xml, int rangeStart, int rangeEnd, String tag) {
        int len = tag.length();
        for (int cursor = rangeStart; cursor < rangeEnd; ) {
            int lt = xml.indexOf('<', cursor);
            if (lt == -1 || lt >= rangeEnd) return null;
            if (lt + 1 + len >= rangeEnd) return null;

            boolean nameMatches = true;
            for (int k = 0; k < len; k++) {
                if (xml.charAt(lt + 1 + k) != tag.charAt(k)) { nameMatches = false; break; }
            }
            char after = xml.charAt(lt + 1 + len);
            if (!nameMatches || (after != ' ' && after != '>' && after != '/' && after != '\t' && after != '\n' && after != '\r')) {
                cursor = lt + 1;
                continue;
            }

            int openEnd = xml.indexOf('>', lt);
            if (openEnd == -1 || openEnd >= rangeEnd) return null;
            if (xml.charAt(openEnd - 1) == '/') return "";

            int closeStart = xml.indexOf("</", openEnd);
            if (closeStart == -1 || closeStart >= rangeEnd) return null;
            String chunk = xml.substring(openEnd + 1, closeStart);
            return chunk.indexOf('&') >= 0 ? unescapeXml(chunk) : chunk;
        }
        return null;
    }

    private static String inlineStringText(String xml) {
        Matcher m = INLINE_STRING_T.matcher(xml);
        StringBuilder sb = null;
        while (m.find()) {
            String chunk = m.group(1);
            if (sb == null) {
                sb = new StringBuilder(chunk.length());
            }
            sb.append(unescapeXml(chunk));
        }
        return sb == null ? null : sb.toString();
    }

    static int cellRefToColumnIndex(String ref) {
        int index = 0;
        for (int i = 0; i < ref.length(); i++) {
            char c = ref.charAt(i);
            if (c >= 'A' && c <= 'Z') index = index * 26 + c - 64;
            else if (c >= 'a' && c <= 'z') index = index * 26 + c - 96;
            else break;
        }
        return Math.max(0, index - 1);
    }

    static String unescapeXml(String value) {
        int amp = value.indexOf('&');
        if (amp == -1) return value;

        int length = value.length();
        StringBuilder sb = new StringBuilder(length);
        if (amp > 0) sb.append(value, 0, amp);
        int cursor = amp;

        while (cursor < length) {
            char c = value.charAt(cursor);
            if (c != '&') {
                sb.append(c);
                cursor++;
                continue;
            }

            int semi = value.indexOf(';', cursor + 1);
            if (semi == -1) {
                sb.append(value, cursor, length);
                break;
            }

            int len = semi - cursor;
            if (len == 4) {
                if (value.charAt(cursor + 1) == 'a' && value.charAt(cursor + 2) == 'm' && value.charAt(cursor + 3) == 'p') {
                    sb.append('&'); cursor = semi + 1; continue;
                }
                if (value.charAt(cursor + 1) == 'l' && value.charAt(cursor + 2) == 't') {
                    sb.append('<'); cursor = semi + 1; continue;
                }
                if (value.charAt(cursor + 1) == 'g' && value.charAt(cursor + 2) == 't') {
                    sb.append('>'); cursor = semi + 1; continue;
                }
            } else if (len == 5) {
                if (value.charAt(cursor + 1) == 'a' && value.charAt(cursor + 2) == 'p'
                        && value.charAt(cursor + 3) == 'o' && value.charAt(cursor + 4) == 's') {
                    sb.append('\''); cursor = semi + 1; continue;
                }
                if (value.charAt(cursor + 1) == 'q' && value.charAt(cursor + 2) == 'u'
                        && value.charAt(cursor + 3) == 'o' && value.charAt(cursor + 4) == 't') {
                    sb.append('"'); cursor = semi + 1; continue;
                }
            }

            if (len > 2 && value.charAt(cursor + 1) == '#') {
                int radix = 10;
                int numStart = cursor + 2;
                if (value.charAt(cursor + 2) == 'x' || value.charAt(cursor + 2) == 'X') {
                    radix = 16;
                    numStart = cursor + 3;
                }
                if (numStart < semi) {
                    try {
                        int cp = Integer.parseInt(value.substring(numStart, semi), radix);
                        sb.appendCodePoint(cp);
                        cursor = semi + 1;
                        continue;
                    } catch (NumberFormatException ignored) {}
                }
            }

            sb.append(value, cursor, semi + 1);
            cursor = semi + 1;
        }
        return sb.toString();
    }

    private static List<SheetEntry> resolveSheets(Map<String, byte[]> files) {
        byte[] wbFile = files.get("xl/workbook.xml");
        byte[] relsFile = files.get("xl/_rels/workbook.xml.rels");

        if (wbFile != null && relsFile != null) {
            String wbXml = new String(wbFile, StandardCharsets.UTF_8);
            String relXml = new String(relsFile, StandardCharsets.UTF_8);
            Map<String, String> rels = parseRelationships(relXml);
            return parseWorkbookSheets(wbXml, rels);
        }

        List<String> paths = new ArrayList<>();
        for (String key : files.keySet()) {
            if (key.matches("xl/worksheets/sheet\\d+\\.xml")) {
                paths.add(key);
            }
        }
        Collections.sort(paths);
        if (paths.isEmpty()) {
            throw new IllegalStateException("No worksheets found in XLSX file");
        }
        List<SheetEntry> entries = new ArrayList<>();
        for (int i = 0; i < paths.size(); i++) {
            entries.add(new SheetEntry("Sheet" + (i + 1), paths.get(i)));
        }
        return entries;
    }

    private static Map<String, String> parseRelationships(String xml) {
        Map<String, String> map = new LinkedHashMap<>();
        int cursor = 0;
        while (cursor < xml.length()) {
            int start = xml.indexOf("<Relationship", cursor);
            if (start == -1) break;
            int end = xml.indexOf(">", start);
            if (end == -1) break;
            String tag = xml.substring(start, end + 1);
            String id = readXmlAttribute(tag, "Id");
            String target = readXmlAttribute(tag, "Target");
            if (id != null && target != null) {
                map.put(id, normalizeWorksheetTarget(target));
            }
            cursor = end + 1;
        }
        return map;
    }

    private static List<SheetEntry> parseWorkbookSheets(String xml, Map<String, String> rels) {
        List<SheetEntry> sheets = new ArrayList<>();
        int cursor = 0;
        int index = 0;
        while (cursor < xml.length()) {
            int start = xml.indexOf("<sheet", cursor);
            if (start == -1) break;
            if (start + 6 < xml.length()) {
                char next = xml.charAt(start + 6);
                if (next != ' ' && next != '/' && next != '>') {
                    cursor = start + 6;
                    continue;
                }
            }
            int end = xml.indexOf(">", start);
            if (end == -1) break;
            String tag = xml.substring(start, end + 1);
            String name = readXmlAttribute(tag, "name");
            if (name == null) name = "Sheet" + (index + 1);
            String rId = readXmlAttribute(tag, "r:id");
            String path = rId != null ? rels.get(rId) : null;
            if (path == null) path = "xl/worksheets/sheet" + (index + 1) + ".xml";
            sheets.add(new SheetEntry(name, path));
            index++;
            cursor = end + 1;
        }
        return sheets;
    }

    private static String normalizeWorksheetTarget(String target) {
        if (target.startsWith("/")) return target.substring(1);
        if (target.startsWith("xl/")) return target;
        return "xl/" + target;
    }

    private static SheetEntry findTarget(List<SheetEntry> entries, ReadOptions options) {
        if (options.getSheetName() != null) {
            for (SheetEntry e : entries) {
                if (e.name.equals(options.getSheetName())) return e;
            }
            throw new IllegalArgumentException("Worksheet not found: " + options.getSheetName());
        }
        int idx = options.getSheetIndex() != null ? options.getSheetIndex() : 0;
        if (idx < 0 || idx >= entries.size()) {
            throw new IllegalArgumentException("Worksheet not found: " + idx);
        }
        return entries.get(idx);
    }

    private static LazySharedStrings buildSharedStrings(Map<String, byte[]> files) {
        byte[] ssFile = files.get("xl/sharedStrings.xml");
        return ssFile != null ? new LazySharedStrings(new String(ssFile, StandardCharsets.UTF_8)) : new LazySharedStrings(null);
    }

    private static Map<String, byte[]> unzip(byte[] data) throws IOException {
        Map<String, byte[]> result = new LinkedHashMap<>();
        try (ZipInputStream zis = new ZipInputStream(new ByteArrayInputStream(data))) {
            ZipEntry entry;
            while ((entry = zis.getNextEntry()) != null) {
                String name = entry.getName();
                String normalized = name.startsWith("/") ? name.substring(1) : name;
                byte[] content = IOUtils.readAllBytes(zis);
                result.put(name, content);
                if (!name.equals(normalized)) {
                    result.put(normalized, content);
                }
            }
        }
        return result;
    }

    static class SheetEntry {
        final String name;
        final String path;

        SheetEntry(String name, String path) {
            this.name = name;
            this.path = path;
        }
    }

    private static class LazySharedStrings {
        private int[] offsets;
        private final Map<Integer, String> cache = new HashMap<>();
        private final String xml;

        LazySharedStrings(String xml) {
            this.xml = xml;
            if (xml == null) {
                this.offsets = new int[0];
            }
        }

        String get(int index) {
            ensureIndex();
            String cached = cache.get(index);
            if (cached != null) return cached;

            if (index < 0 || index >= offsets.length) return "";

            int start = offsets[index];
            int openEnd = xml.indexOf(">", start);
            if (openEnd == -1) return "";
            int end = xml.indexOf("</si>", openEnd);
            if (end == -1) return "";
            String value = inlineStringText(xml.substring(openEnd + 1, end));
            if (value == null) value = "";
            cache.put(index, value);
            return value;
        }

        private void ensureIndex() {
            if (offsets != null) return;
            List<Integer> list = new ArrayList<>();
            int cursor = 0;
            while (cursor < xml.length()) {
                int pos = xml.indexOf("<si", cursor);
                if (pos == -1) break;
                list.add(pos);
                int end = xml.indexOf("</si>", pos);
                if (end == -1) break;
                cursor = end + 5;
            }
            offsets = new int[list.size()];
            for (int i = 0; i < list.size(); i++) offsets[i] = list.get(i);
        }
    }
}
