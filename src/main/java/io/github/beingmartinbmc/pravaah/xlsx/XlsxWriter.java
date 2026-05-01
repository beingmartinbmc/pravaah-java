package io.github.beingmartinbmc.pravaah.xlsx;

import io.github.beingmartinbmc.pravaah.Row;
import io.github.beingmartinbmc.pravaah.WriteOptions;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.zip.*;

public final class XlsxWriter {

    private XlsxWriter() {}

    public static void writeWorkbook(Workbook book, String destination) throws IOException {
        writeWorkbook(book, destination, WriteOptions.defaults());
    }

    public static void writeWorkbook(Workbook book, String destination, WriteOptions options) throws IOException {
        List<Worksheet> sheets = book.getSheets();
        if (sheets.isEmpty()) {
            sheets = new ArrayList<>();
            sheets.add(new Worksheet(options.getSheetName() != null ? options.getSheetName() : "Sheet1"));
        }

        FileOutputStream fos = new FileOutputStream(destination);
        ZipOutputStream zos = new ZipOutputStream(fos);
        try {
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
        } finally {
            zos.close();
        }
    }

    public static void writeRows(List<Row> rows, String destination, WriteOptions options) throws IOException {
        String sheetName = options.getSheetName() != null ? options.getSheetName() : "Sheet1";
        List<String> headers = options.getHeaders();
        if (headers == null) headers = inferHeaders(rows);

        Worksheet ws = new Worksheet(sheetName, rows);
        Workbook wb = new Workbook(new ArrayList<>(Arrays.asList(ws)));
        writeWorkbook(wb, destination, options);
    }

    private static void writeWorksheetEntry(ZipOutputStream zos, String name, Worksheet sheet,
                                            List<String> headers) throws IOException {
        zos.putNextEntry(new ZipEntry(name));
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(zos, StandardCharsets.UTF_8), 64 * 1024);
        writeWorksheetXml(writer, sheet, headers);
        writer.flush();
        zos.closeEntry();
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

    private static void writeWorksheetXml(Writer writer, Worksheet sheet, List<String> headers) throws IOException {
        writer.write("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n");
        writer.write("<worksheet xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\">\n");

        if (sheet != null && sheet.getFrozen() != null) {
            FreezePane f = sheet.getFrozen();
            writer.write("  <sheetViews><sheetView workbookViewId=\"0\"><pane ");
            if (f.getXSplit() != null) {
                writer.write("xSplit=\"");
                writer.write(String.valueOf(f.getXSplit()));
                writer.write("\" ");
            }
            if (f.getYSplit() != null) {
                writer.write("ySplit=\"");
                writer.write(String.valueOf(f.getYSplit()));
                writer.write("\" ");
            }
            if (f.getTopLeftCell() != null) {
                writer.write("topLeftCell=\"");
                writer.write(escapeXml(f.getTopLeftCell()));
                writer.write("\" ");
            }
            writer.write("state=\"frozen\"/></sheetView></sheetViews>\n");
        }

        if (sheet != null && !sheet.getColumns().isEmpty()) {
            writer.write("  <cols>");
            for (int i = 0; i < sheet.getColumns().size(); i++) {
                ColumnDefinition col = sheet.getColumns().get(i);
                writer.write("<col min=\"");
                writer.write(String.valueOf(i + 1));
                writer.write("\" max=\"");
                writer.write(String.valueOf(i + 1));
                writer.write("\" width=\"");
                writer.write(String.valueOf(col.getWidth()));
                writer.write("\" customWidth=\"1\"/>");
            }
            writer.write("</cols>\n");
        }

        String[] headerArr = headers.toArray(new String[0]);
        String[] columnNames = columnNames(headerArr.length);

        writer.write("  <sheetData>\n");
        writeRow(writer, 1, headerArr, columnNames);
        List<Row> rows = sheet != null ? sheet.getRows() : Collections.<Row>emptyList();
        for (int r = 0; r < rows.size(); r++) {
            Row row = rows.get(r);
            int rowNumber = r + 2;
            writer.write("    <row r=\"");
            writer.write(String.valueOf(rowNumber));
            writer.write("\">");
            for (int c = 0; c < headerArr.length; c++) {
                writeCellXml(writer, row.get(headerArr[c]), columnNames[c], rowNumber);
            }
            writer.write("</row>\n");
        }
        writer.write("  </sheetData>\n");

        if (sheet != null && !sheet.getTables().isEmpty()) {
            writer.write("  <autoFilter ref=\"");
            writer.write(escapeXml(sheet.getTables().get(0).getRange()));
            writer.write("\"/>\n");
        }
        if (sheet != null && !sheet.getMerges().isEmpty()) {
            writer.write("  <mergeCells count=\"");
            writer.write(String.valueOf(sheet.getMerges().size()));
            writer.write("\">");
            for (String merge : sheet.getMerges()) {
                writer.write("<mergeCell ref=\"");
                writer.write(escapeXml(merge));
                writer.write("\"/>");
            }
            writer.write("</mergeCells>\n");
        }
        if (sheet != null && !sheet.getValidations().isEmpty()) {
            writer.write("  <dataValidations count=\"");
            writer.write(String.valueOf(sheet.getValidations().size()));
            writer.write("\">");
            for (DataValidation dv : sheet.getValidations()) {
                writer.write("<dataValidation type=\"");
                writer.write(dv.getType());
                writer.write("\" sqref=\"");
                writer.write(escapeXml(dv.getRange()));
                writer.write("\">");
                if (dv.getFormula() != null) {
                    writer.write("<formula1>");
                    writer.write(escapeXml(dv.getFormula()));
                    writer.write("</formula1>");
                }
                writer.write("</dataValidation>");
            }
            writer.write("</dataValidations>\n");
        }

        writer.write("</worksheet>");
    }

    private static void writeRow(Writer writer, int rowNumber, Object[] values, String[] columnNames) throws IOException {
        writer.write("    <row r=\"");
        writer.write(String.valueOf(rowNumber));
        writer.write("\">");
        for (int c = 0; c < values.length; c++) {
            writeCellXml(writer, values[c], columnNames[c], rowNumber);
        }
        writer.write("</row>\n");
    }

    private static void writeCellXml(Writer writer, Object value, String columnName, int rowNumber) throws IOException {
        writer.write("<c r=\"");
        writer.write(columnName);
        writer.write(String.valueOf(rowNumber));
        writer.write("\"");
        if (value instanceof FormulaCell) {
            FormulaCell fc = (FormulaCell) value;
            String f = fc.getFormula().startsWith("=") ? fc.getFormula().substring(1) : fc.getFormula();
            writer.write("><f>");
            writer.write(escapeXml(f));
            writer.write("</f>");
            if (fc.getResult() != null) {
                writer.write("<v>");
                writer.write(escapeXml(String.valueOf(fc.getResult())));
                writer.write("</v>");
            }
            writer.write("</c>");
            return;
        }
        if (value == null) {
            writer.write("/>");
            return;
        }
        if (value instanceof Number) {
            writer.write("><v>");
            writer.write(String.valueOf(value));
            writer.write("</v></c>");
            return;
        }
        if (value instanceof Boolean) {
            writer.write(" t=\"b\"><v>");
            writer.write((Boolean) value ? "1" : "0");
            writer.write("</v></c>");
            return;
        }
        writer.write(" t=\"inlineStr\"><is><t>");
        if (value instanceof Date) {
            writer.write(escapeXml(((Date) value).toInstant().toString()));
        } else {
            writer.write(escapeXml(String.valueOf(value)));
        }
        writer.write("</t></is></c>");
    }

    private static String[] columnNames(int count) {
        String[] names = new String[count];
        for (int i = 0; i < count; i++) {
            names[i] = columnName(i + 1);
        }
        return names;
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
        return value
                .replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
                .replace("\"", "&quot;");
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
