package io.github.beingmartinbmc.pravaah.xlsx;

import io.github.beingmartinbmc.pravaah.ReadOptions;
import io.github.beingmartinbmc.pravaah.Row;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.*;
import java.util.zip.*;

public final class XlsxReader {

    private XlsxReader() {}

    public static List<Row> readAll(String filePath, ReadOptions options) throws IOException {
        return readAll(readBytes(filePath), options);
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
        return readWorkbook(readBytes(filePath), options);
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

        List<Object[]> valueRows = parseSheetDataRows(xml, ss, options);
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
                    if (values[i] != null) row.put("_" + (i + 1), values[i]);
                    else row.put("_" + (i + 1), values[i]);
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

    private static List<Object[]> parseSheetDataRows(String xml, LazySharedStrings ss, ReadOptions options) {
        List<Object[]> rows = new ArrayList<>();

        int sdStart = xml.indexOf("<sheetData");
        if (sdStart == -1) return rows;
        int sdOpenEnd = xml.indexOf(">", sdStart);
        int sdEnd = xml.indexOf("</sheetData>", sdOpenEnd);
        if (sdOpenEnd == -1 || sdEnd == -1) return rows;

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
                rows.add(new Object[0]);
                continue;
            }

            int rowEnd = xml.indexOf("</row>", rowOpenEnd);
            if (rowEnd == -1) break;

            String rowXml = xml.substring(rowOpenEnd + 1, rowEnd);
            rows.add(parseRowCells(rowXml, ss, options, colCount));
            cursor = rowEnd + 6;
        }

        return rows;
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

    private static Object[] parseRowCells(String rowXml, LazySharedStrings ss, ReadOptions options, Integer colCount) {
        Object[] values;
        if (colCount != null) {
            values = new Object[colCount];
        } else {
            values = new Object[0];
        }

        List<Object> dynamicValues = colCount == null ? new ArrayList<>() : null;
        int cursor = 0;
        int implicitColumn = 0;

        while (cursor < rowXml.length()) {
            int cellStart = rowXml.indexOf("<c", cursor);
            if (cellStart == -1) break;

            int cellOpenEnd = rowXml.indexOf(">", cellStart);
            if (cellOpenEnd == -1) break;

            String openTag = rowXml.substring(cellStart, cellOpenEnd + 1);
            String ref = readXmlAttribute(openTag, "r");
            String type = readXmlAttribute(openTag, "t");
            int columnIndex = ref != null ? cellRefToColumnIndex(ref) : implicitColumn;
            implicitColumn = columnIndex + 1;

            if (openTag.endsWith("/>")) {
                setValueAt(values, dynamicValues, columnIndex, null);
                cursor = cellOpenEnd + 1;
                continue;
            }

            int cellEnd = rowXml.indexOf("</c>", cellOpenEnd);
            if (cellEnd == -1) break;

            String innerXml = rowXml.substring(cellOpenEnd + 1, cellEnd);
            Object cellValue = decodeCellValue(type, innerXml, ss, options);
            setValueAt(values, dynamicValues, columnIndex, cellValue);
            cursor = cellEnd + 4;
        }

        if (dynamicValues != null) {
            return dynamicValues.toArray(new Object[0]);
        }
        return values;
    }

    private static void setValueAt(Object[] fixedArray, List<Object> dynamicList, int index, Object value) {
        if (dynamicList != null) {
            while (dynamicList.size() <= index) dynamicList.add(null);
            dynamicList.set(index, value);
        } else if (index < fixedArray.length) {
            fixedArray[index] = value;
        }
    }

    private static Object decodeCellValue(String type, String innerXml, LazySharedStrings ss, ReadOptions options) {
        String formulaText = firstTagText(innerXml, "f");
        if (formulaText != null && "preserve".equals(options.getFormulas())) {
            String f = formulaText.startsWith("=") ? formulaText.substring(1) : formulaText;
            ReadOptions valuesOpt = new ReadOptions().formulas("values");
            Object result = decodeCellValue(type, innerXml, ss, valuesOpt);
            return new FormulaCell(f, result);
        }

        if ("s".equals(type)) {
            String vText = firstTagText(innerXml, "v");
            if (vText == null) return null;
            int idx = Integer.parseInt(vText);
            return ss.get(idx);
        }
        if ("inlineStr".equals(type)) {
            return inlineStringText(innerXml);
        }
        if ("b".equals(type)) {
            return "1".equals(firstTagText(innerXml, "v"));
        }

        String rawValue = firstTagText(innerXml, "v");
        if (rawValue == null) return null;
        try {
            double d = Double.parseDouble(rawValue);
            if (Double.isFinite(d)) {
                if (d == Math.floor(d) && !rawValue.contains(".") && d >= Integer.MIN_VALUE && d <= Integer.MAX_VALUE) {
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

    private static String firstTagText(String xml, String tag) {
        int start = xml.indexOf("<" + tag);
        if (start == -1) return null;
        int openEnd = xml.indexOf(">", start);
        if (openEnd == -1) return null;
        int end = xml.indexOf("</" + tag + ">", openEnd);
        return end == -1 ? null : unescapeXml(xml.substring(openEnd + 1, end));
    }

    private static String inlineStringText(String xml) {
        Pattern p = Pattern.compile("<t\\b[^>]*>([\\s\\S]*?)</t>");
        Matcher m = p.matcher(xml);
        StringBuilder sb = new StringBuilder();
        while (m.find()) {
            sb.append(unescapeXml(m.group(1)));
        }
        return sb.length() == 0 ? null : sb.toString();
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
        if (value.indexOf('&') == -1) return value;
        value = value.replaceAll("&#x([0-9a-fA-F]+);", "");
        Matcher hexMatcher = Pattern.compile("&#x([0-9a-fA-F]+);").matcher(value);
        StringBuffer sb = new StringBuffer();

        String original = value;
        hexMatcher = Pattern.compile("&#x([0-9a-fA-F]+);").matcher(original);
        sb = new StringBuffer();
        while (hexMatcher.find()) {
            int cp = Integer.parseInt(hexMatcher.group(1), 16);
            hexMatcher.appendReplacement(sb, Matcher.quoteReplacement(new String(Character.toChars(cp))));
        }
        hexMatcher.appendTail(sb);
        value = sb.toString();

        Matcher decMatcher = Pattern.compile("&#(\\d+);").matcher(value);
        sb = new StringBuffer();
        while (decMatcher.find()) {
            int cp = Integer.parseInt(decMatcher.group(1));
            decMatcher.appendReplacement(sb, Matcher.quoteReplacement(new String(Character.toChars(cp))));
        }
        decMatcher.appendTail(sb);
        value = sb.toString();

        value = value.replace("&quot;", "\"");
        value = value.replace("&apos;", "'");
        value = value.replace("&lt;", "<");
        value = value.replace("&gt;", ">");
        value = value.replace("&amp;", "&");
        return value;
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
        ZipInputStream zis = new ZipInputStream(new ByteArrayInputStream(data));
        try {
            ZipEntry entry;
            while ((entry = zis.getNextEntry()) != null) {
                String name = entry.getName();
                String normalized = name.startsWith("/") ? name.substring(1) : name;
                byte[] content = readStreamFully(zis);
                result.put(name, content);
                if (!name.equals(normalized)) {
                    result.put(normalized, content);
                }
            }
        } finally {
            zis.close();
        }
        return result;
    }

    private static byte[] readStreamFully(InputStream is) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        byte[] buf = new byte[4096];
        int read;
        while ((read = is.read(buf)) != -1) {
            baos.write(buf, 0, read);
        }
        return baos.toByteArray();
    }

    private static byte[] readBytes(String path) throws IOException {
        FileInputStream fis = new FileInputStream(path);
        try {
            return readStreamFully(fis);
        } finally {
            fis.close();
        }
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
