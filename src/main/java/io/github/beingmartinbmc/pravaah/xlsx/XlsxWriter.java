package io.github.beingmartinbmc.pravaah.xlsx;

import io.github.beingmartinbmc.pravaah.Row;
import io.github.beingmartinbmc.pravaah.WriteOptions;
import io.github.beingmartinbmc.pravaah.internal.io.IOUtils;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public final class XlsxWriter {

    private XlsxWriter() {}

    public static void writeWorkbook(Workbook book, String destination) throws IOException {
        writeWorkbook(book, destination, WriteOptions.defaults());
    }

    public static void writeWorkbook(Workbook book, String destination, WriteOptions options) throws IOException {
        try (FileOutputStream fos = new FileOutputStream(destination)) {
            writeWorkbook(book, fos, options);
        }
    }

    public static void writeWorkbook(Workbook book, OutputStream output, WriteOptions options) throws IOException {
        List<Worksheet> sheets = book.getSheets();
        if (sheets.isEmpty()) {
            sheets = new ArrayList<>();
            sheets.add(new Worksheet(options.getSheetName() != null ? options.getSheetName() : "Sheet1"));
        }

        ZipOutputStream zos = new ZipOutputStream(output);
        zos.setLevel(1);
        writeEntry(zos, "[Content_Types].xml", contentTypesXml(sheets));
        writeEntry(zos, "_rels/.rels", rootRelsXml());
        writeEntry(zos, "docProps/app.xml", appXml(sheets));
        writeEntry(zos, "docProps/core.xml", coreXml(book.getProperties()));
        writeEntry(zos, "xl/workbook.xml", workbookXml(sheets));
        writeEntry(zos, "xl/_rels/workbook.xml.rels", workbookRelsXml(sheets));
        writeEntry(zos, "xl/styles.xml", stylesXml());

        for (int i = 0; i < sheets.size(); i++) {
            Worksheet sheet = sheets.get(i);
            List<String> headers = options.getHeaders();
            if (headers == null) headers = inferHeaders(sheet.getRows());
            writeWorksheetEntry(zos, "xl/worksheets/sheet" + (i + 1) + ".xml", sheet, headers);
        }
        zos.finish();
        zos.flush();
    }

    public static void writeRows(List<Row> rows, String destination, WriteOptions options) throws IOException {
        try (FileOutputStream fos = new FileOutputStream(destination)) {
            writeRows(rows, fos, options);
        }
    }

    public static void writeRows(List<Row> rows, OutputStream output, WriteOptions options) throws IOException {
        String sheetName = options.getSheetName() != null ? options.getSheetName() : "Sheet1";
        List<String> headers = options.getHeaders();
        if (headers == null) headers = inferHeaders(rows);

        Worksheet ws = new Worksheet(sheetName, rows);
        Workbook wb = new Workbook(new ArrayList<>(Arrays.asList(ws)));
        writeWorkbook(wb, output, options);
    }

    private static void writeWorksheetEntry(ZipOutputStream zos, String name, Worksheet sheet,
                                            List<String> headers) throws IOException {
        zos.putNextEntry(new ZipEntry(name));
        XmlByteSink sink = new XmlByteSink(zos, IOUtils.LARGE_IO_BUFFER_SIZE);
        try {
            writeWorksheetXml(sink, sheet, headers);
            sink.flush();
        } finally {
            zos.closeEntry();
        }
    }

    private static void writeEntry(ZipOutputStream zos, String name, String content) throws IOException {
        zos.putNextEntry(new ZipEntry(name));
        zos.write(content.getBytes(StandardCharsets.UTF_8));
        zos.closeEntry();
    }

    private static List<String> inferHeaders(List<Row> rows) {
        if (rows.isEmpty()) return Collections.emptyList();
        return new ArrayList<>(rows.get(0).keySet());
    }

    private static void writeWorksheetXml(XmlByteSink writer, Worksheet sheet, List<String> headers) throws IOException {
        writer.writeAscii("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n");
        writer.writeAscii("<worksheet xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\">\n");

        if (sheet != null && sheet.getFrozen() != null) {
            FreezePane f = sheet.getFrozen();
            writer.writeAscii("  <sheetViews><sheetView workbookViewId=\"0\"><pane ");
            if (f.getXSplit() != null) {
                writer.writeAscii("xSplit=\"");
                writer.writeAscii(String.valueOf(f.getXSplit()));
                writer.writeAscii("\" ");
            }
            if (f.getYSplit() != null) {
                writer.writeAscii("ySplit=\"");
                writer.writeAscii(String.valueOf(f.getYSplit()));
                writer.writeAscii("\" ");
            }
            if (f.getTopLeftCell() != null) {
                writer.writeAscii("topLeftCell=\"");
                writer.writeEscaped(f.getTopLeftCell());
                writer.writeAscii("\" ");
            }
            writer.writeAscii("state=\"frozen\"/></sheetView></sheetViews>\n");
        }

        if (sheet != null && !sheet.getColumns().isEmpty()) {
            writer.writeAscii("  <cols>");
            for (int i = 0; i < sheet.getColumns().size(); i++) {
                ColumnDefinition col = sheet.getColumns().get(i);
                writer.writeAscii("<col min=\"");
                writer.writeAscii(String.valueOf(i + 1));
                writer.writeAscii("\" max=\"");
                writer.writeAscii(String.valueOf(i + 1));
                writer.writeAscii("\" width=\"");
                writer.writeAscii(String.valueOf(col.getWidth()));
                writer.writeAscii("\" customWidth=\"1\"/>");
            }
            writer.writeAscii("</cols>\n");
        }

        String[] headerArr = headers.toArray(new String[0]);
        byte[][] columnPrefixes = columnNameBytes(headerArr.length);

        writer.writeAscii("  <sheetData>\n");
        writeHeaderRow(writer, 1, headerArr, columnPrefixes);
        List<Row> rows = sheet != null ? sheet.getRows() : Collections.<Row>emptyList();
        for (int r = 0; r < rows.size(); r++) {
            Row row = rows.get(r);
            int rowNumber = r + 2;
            writer.writeAscii("    <row r=\"");
            writer.writeIntAscii(rowNumber);
            writer.writeAscii("\">");
            for (int c = 0; c < headerArr.length; c++) {
                writeCellXml(writer, row.get(headerArr[c]), columnPrefixes[c], rowNumber);
            }
            writer.writeAscii("</row>\n");
        }
        writer.writeAscii("  </sheetData>\n");

        if (sheet != null && !sheet.getTables().isEmpty()) {
            writer.writeAscii("  <autoFilter ref=\"");
            writer.writeEscaped(sheet.getTables().get(0).getRange());
            writer.writeAscii("\"/>\n");
        }
        if (sheet != null && !sheet.getMerges().isEmpty()) {
            writer.writeAscii("  <mergeCells count=\"");
            writer.writeAscii(String.valueOf(sheet.getMerges().size()));
            writer.writeAscii("\">");
            for (String merge : sheet.getMerges()) {
                writer.writeAscii("<mergeCell ref=\"");
                writer.writeEscaped(merge);
                writer.writeAscii("\"/>");
            }
            writer.writeAscii("</mergeCells>\n");
        }
        if (sheet != null && !sheet.getValidations().isEmpty()) {
            writer.writeAscii("  <dataValidations count=\"");
            writer.writeAscii(String.valueOf(sheet.getValidations().size()));
            writer.writeAscii("\">");
            for (DataValidation dv : sheet.getValidations()) {
                writer.writeAscii("<dataValidation type=\"");
                writer.writeAscii(dv.getType());
                writer.writeAscii("\" sqref=\"");
                writer.writeEscaped(dv.getRange());
                writer.writeAscii("\">");
                if (dv.getFormula() != null) {
                    writer.writeAscii("<formula1>");
                    writer.writeEscaped(dv.getFormula());
                    writer.writeAscii("</formula1>");
                }
                writer.writeAscii("</dataValidation>");
            }
            writer.writeAscii("</dataValidations>\n");
        }

        writer.writeAscii("</worksheet>");
    }

    private static void writeHeaderRow(XmlByteSink writer, int rowNumber, Object[] values, byte[][] columnPrefixes) throws IOException {
        writer.writeAscii("    <row r=\"");
        writer.writeIntAscii(rowNumber);
        writer.writeAscii("\">");
        for (int c = 0; c < values.length; c++) {
            writeCellXml(writer, values[c], columnPrefixes[c], rowNumber);
        }
        writer.writeAscii("</row>\n");
    }

    private static void writeCellXml(XmlByteSink writer, Object value, byte[] columnPrefix, int rowNumber) throws IOException {
        writer.writeBytes(columnPrefix);
        writer.writeIntAscii(rowNumber);
        writer.writeAscii("\"");
        if (value instanceof FormulaCell) {
            FormulaCell fc = (FormulaCell) value;
            String f = fc.getFormula().startsWith("=") ? fc.getFormula().substring(1) : fc.getFormula();
            writer.writeAscii("><f>");
            writer.writeEscaped(f);
            writer.writeAscii("</f>");
            if (fc.getResult() != null) {
                writer.writeAscii("<v>");
                writer.writeEscaped(String.valueOf(fc.getResult()));
                writer.writeAscii("</v>");
            }
            writer.writeAscii("</c>");
            return;
        }
        if (value == null) {
            writer.writeAscii("/>");
            return;
        }
        if (value instanceof Number) {
            writer.writeAscii("><v>");
            writer.writeAscii(String.valueOf(value));
            writer.writeAscii("</v></c>");
            return;
        }
        if (value instanceof Boolean) {
            writer.writeAscii(" t=\"b\"><v>");
            writer.writeAscii((Boolean) value ? "1" : "0");
            writer.writeAscii("</v></c>");
            return;
        }
        writer.writeAscii(" t=\"inlineStr\"><is><t>");
        if (value instanceof Date) {
            writer.writeEscaped(((Date) value).toInstant().toString());
        } else {
            writer.writeEscaped(String.valueOf(value));
        }
        writer.writeAscii("</t></is></c>");
    }

    private static byte[][] columnNameBytes(int count) {
        byte[][] prefixes = new byte[count][];
        byte[] before = "<c r=\"".getBytes(StandardCharsets.US_ASCII);
        for (int i = 0; i < count; i++) {
            byte[] name = columnName(i + 1).getBytes(StandardCharsets.US_ASCII);
            byte[] combined = new byte[before.length + name.length];
            System.arraycopy(before, 0, combined, 0, before.length);
            System.arraycopy(name, 0, combined, before.length, name.length);
            prefixes[i] = combined;
        }
        return prefixes;
    }

    static String columnName(int index) {
        StringBuilder name = new StringBuilder();
        int current = index;
        while (current > 0) {
            int remainder = (current - 1) % 26;
            name.insert(0, (char) (65 + remainder));
            current = (current - 1) / 26;
        }
        return name.toString();
    }

    static String escapeXml(String value) {
        if (value == null) return "";
        int length = value.length();
        boolean needs = false;
        for (int i = 0; i < length; i++) {
            char c = value.charAt(i);
            if (c == '&' || c == '<' || c == '>' || c == '"') { needs = true; break; }
        }
        if (!needs) return value;
        StringBuilder sb = new StringBuilder(length + 16);
        for (int i = 0; i < length; i++) {
            char c = value.charAt(i);
            switch (c) {
                case '&': sb.append("&amp;"); break;
                case '<': sb.append("&lt;"); break;
                case '>': sb.append("&gt;"); break;
                case '"': sb.append("&quot;"); break;
                default: sb.append(c);
            }
        }
        return sb.toString();
    }

    /**
     * Direct byte-oriented XML sink with an ASCII fast path. Avoids the
     * per-char encoding overhead of {@code OutputStreamWriter} and supports
     * escaping XML special characters in a single pass.
     */
    static final class XmlByteSink {
        private final OutputStream out;
        private final byte[] buffer;
        private int pos;

        XmlByteSink(OutputStream out, int capacity) {
            this.out = out;
            this.buffer = new byte[capacity];
        }

        void writeAscii(String value) throws IOException {
            int length = value.length();
            int srcPos = 0;
            while (srcPos < length) {
                int free = buffer.length - pos;
                if (free == 0) {
                    drain();
                    free = buffer.length;
                }
                int chunk = Math.min(free, length - srcPos);
                int chunkEnd = srcPos + chunk;
                for (int i = srcPos; i < chunkEnd; i++) {
                    buffer[pos++] = (byte) value.charAt(i);
                }
                srcPos = chunkEnd;
            }
        }

        void writeBytes(byte[] bytes) throws IOException {
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

        void writeIntAscii(int value) throws IOException {
            if (value == 0) { writeByte((byte) '0'); return; }
            if (value == Integer.MIN_VALUE) {
                writeAscii("-2147483648");
                return;
            }
            byte[] tmp = new byte[11];
            int t = 0;
            int v = value;
            boolean negative = v < 0;
            if (negative) v = -v;
            while (v > 0) {
                tmp[t++] = (byte) ('0' + (v % 10));
                v /= 10;
            }
            if (t + (negative ? 1 : 0) > buffer.length - pos) drain();
            if (negative) buffer[pos++] = (byte) '-';
            for (int i = t - 1; i >= 0; i--) buffer[pos++] = tmp[i];
        }

        void writeEscaped(String value) throws IOException {
            if (value == null || value.isEmpty()) return;
            int length = value.length();
            boolean asciiSafe = true;
            for (int i = 0; i < length; i++) {
                char c = value.charAt(i);
                if (c > 0x7F) { asciiSafe = false; break; }
            }
            if (!asciiSafe) {
                writeBytes(escapeXml(value).getBytes(StandardCharsets.UTF_8));
                return;
            }
            int start = 0;
            for (int i = 0; i < length; i++) {
                char c = value.charAt(i);
                String esc = null;
                if (c == '&') esc = "&amp;";
                else if (c == '<') esc = "&lt;";
                else if (c == '>') esc = "&gt;";
                else if (c == '"') esc = "&quot;";
                if (esc == null) continue;
                if (i > start) writeAsciiRange(value, start, i);
                writeAscii(esc);
                start = i + 1;
            }
            if (start < length) writeAsciiRange(value, start, length);
        }

        private void writeAsciiRange(String value, int start, int end) throws IOException {
            int srcPos = start;
            while (srcPos < end) {
                int free = buffer.length - pos;
                if (free == 0) {
                    drain();
                    free = buffer.length;
                }
                int chunk = Math.min(free, end - srcPos);
                int chunkEnd = srcPos + chunk;
                for (int i = srcPos; i < chunkEnd; i++) {
                    buffer[pos++] = (byte) value.charAt(i);
                }
                srcPos = chunkEnd;
            }
        }

        void writeByte(byte b) throws IOException {
            if (pos == buffer.length) drain();
            buffer[pos++] = b;
        }

        void flush() throws IOException {
            drain();
        }

        private void drain() throws IOException {
            if (pos > 0) {
                out.write(buffer, 0, pos);
                pos = 0;
            }
        }
    }

    private static String contentTypesXml(List<Worksheet> sheets) {
        StringBuilder sb = new StringBuilder();
        sb.append("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n");
        sb.append("<Types xmlns=\"http://schemas.openxmlformats.org/package/2006/content-types\">\n");
        sb.append("  <Default Extension=\"rels\" ContentType=\"application/vnd.openxmlformats-package.relationships+xml\"/>\n");
        sb.append("  <Default Extension=\"xml\" ContentType=\"application/xml\"/>\n");
        sb.append("  <Override PartName=\"/xl/workbook.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml\"/>\n");
        for (int i = 0; i < sheets.size(); i++) {
            sb.append("  <Override PartName=\"/xl/worksheets/sheet").append(i + 1)
                    .append(".xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml\"/>\n");
        }
        sb.append("  <Override PartName=\"/xl/styles.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml\"/>\n");
        sb.append("  <Override PartName=\"/docProps/core.xml\" ContentType=\"application/vnd.openxmlformats-package.core-properties+xml\"/>\n");
        sb.append("  <Override PartName=\"/docProps/app.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.extended-properties+xml\"/>\n");
        sb.append("</Types>");
        return sb.toString();
    }

    private static String rootRelsXml() {
        return "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n"
                + "<Relationships xmlns=\"http://schemas.openxmlformats.org/package/2006/relationships\">\n"
                + "  <Relationship Id=\"rId1\" Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument\" Target=\"xl/workbook.xml\"/>\n"
                + "  <Relationship Id=\"rId2\" Type=\"http://schemas.openxmlformats.org/package/2006/relationships/metadata/core-properties\" Target=\"docProps/core.xml\"/>\n"
                + "  <Relationship Id=\"rId3\" Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/extended-properties\" Target=\"docProps/app.xml\"/>\n"
                + "</Relationships>";
    }

    private static String workbookXml(List<Worksheet> sheets) {
        StringBuilder sb = new StringBuilder();
        sb.append("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n");
        sb.append("<workbook xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\" xmlns:r=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships\">\n");
        sb.append("  <sheets>");
        for (int i = 0; i < sheets.size(); i++) {
            sb.append("<sheet name=\"").append(escapeXml(sheets.get(i).getName()))
                    .append("\" sheetId=\"").append(i + 1)
                    .append("\" r:id=\"rId").append(i + 1).append("\"/>");
        }
        sb.append("</sheets>\n</workbook>");
        return sb.toString();
    }

    private static String workbookRelsXml(List<Worksheet> sheets) {
        StringBuilder sb = new StringBuilder();
        sb.append("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n");
        sb.append("<Relationships xmlns=\"http://schemas.openxmlformats.org/package/2006/relationships\">\n");
        for (int i = 0; i < sheets.size(); i++) {
            sb.append("  <Relationship Id=\"rId").append(i + 1)
                    .append("\" Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet\" Target=\"worksheets/sheet")
                    .append(i + 1).append(".xml\"/>\n");
        }
        sb.append("  <Relationship Id=\"rId").append(sheets.size() + 1)
                .append("\" Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles\" Target=\"styles.xml\"/>\n");
        sb.append("</Relationships>");
        return sb.toString();
    }

    private static String stylesXml() {
        return "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n"
                + "<styleSheet xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\">\n"
                + "  <fonts count=\"1\"><font><sz val=\"11\"/><name val=\"Calibri\"/></font></fonts>\n"
                + "  <fills count=\"1\"><fill><patternFill patternType=\"none\"/></fill></fills>\n"
                + "  <borders count=\"1\"><border/></borders>\n"
                + "  <cellStyleXfs count=\"1\"><xf numFmtId=\"0\" fontId=\"0\" fillId=\"0\" borderId=\"0\"/></cellStyleXfs>\n"
                + "  <cellXfs count=\"1\"><xf numFmtId=\"0\" fontId=\"0\" fillId=\"0\" borderId=\"0\" xfId=\"0\"/></cellXfs>\n"
                + "</styleSheet>";
    }

    private static String appXml(List<Worksheet> sheets) {
        StringBuilder parts = new StringBuilder();
        for (Worksheet s : sheets) {
            parts.append("<vt:lpstr>").append(escapeXml(s.getName())).append("</vt:lpstr>");
        }
        return "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n"
                + "<Properties xmlns=\"http://schemas.openxmlformats.org/officeDocument/2006/extended-properties\">\n"
                + "  <Application>Pravaah</Application>"
                + "<TitlesOfParts><vt:vector xmlns:vt=\"http://schemas.openxmlformats.org/officeDocument/2006/docPropsVTypes\" size=\""
                + sheets.size() + "\" baseType=\"lpstr\">" + parts + "</vt:vector></TitlesOfParts>\n"
                + "</Properties>";
    }

    private static String coreXml(Map<String, String> properties) {
        String creator = properties != null && properties.containsKey("creator") ? properties.get("creator") : "Pravaah";
        return "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n"
                + "<cp:coreProperties xmlns:cp=\"http://schemas.openxmlformats.org/package/2006/metadata/core-properties\" "
                + "xmlns:dc=\"http://purl.org/dc/elements/1.1/\" xmlns:dcterms=\"http://purl.org/dc/terms/\">\n"
                + "  <dc:creator>" + escapeXml(creator) + "</dc:creator>\n"
                + "</cp:coreProperties>";
    }
}
