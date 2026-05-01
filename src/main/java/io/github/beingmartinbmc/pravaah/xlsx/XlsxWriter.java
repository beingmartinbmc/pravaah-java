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
            zos.setLevel(6);
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
                List<Object[]> dataRows = new ArrayList<>();
                String[] headerArr = headers.toArray(new String[0]);
                dataRows.add(headerArr);
                for (Row row : sheet.getRows()) {
                    Object[] rowData = new Object[headerArr.length];
                    for (int j = 0; j < headerArr.length; j++) {
                        rowData[j] = row.get(headerArr[j]);
                    }
                    dataRows.add(rowData);
                }
                writeEntry(zos, "xl/worksheets/sheet" + (i + 1) + ".xml", worksheetXml(dataRows, sheet));
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

    private static void writeEntry(ZipOutputStream zos, String name, String content) throws IOException {
        zos.putNextEntry(new ZipEntry(name));
        zos.write(content.getBytes(StandardCharsets.UTF_8));
        zos.closeEntry();
    }

    private static List<String> inferHeaders(List<Row> rows) {
        if (rows.isEmpty()) return Collections.emptyList();
        return new ArrayList<>(rows.get(0).keySet());
    }

    private static String worksheetXml(List<Object[]> rows, Worksheet sheet) {
        StringBuilder sb = new StringBuilder();
        sb.append("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n");
        sb.append("<worksheet xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\">\n");

        if (sheet != null && sheet.getFrozen() != null) {
            FreezePane f = sheet.getFrozen();
            sb.append("  <sheetViews><sheetView workbookViewId=\"0\"><pane ");
            if (f.getXSplit() != null) sb.append("xSplit=\"").append(f.getXSplit()).append("\" ");
            if (f.getYSplit() != null) sb.append("ySplit=\"").append(f.getYSplit()).append("\" ");
            if (f.getTopLeftCell() != null) sb.append("topLeftCell=\"").append(escapeXml(f.getTopLeftCell())).append("\" ");
            sb.append("state=\"frozen\"/></sheetView></sheetViews>\n");
        }

        if (sheet != null && !sheet.getColumns().isEmpty()) {
            sb.append("  <cols>");
            for (int i = 0; i < sheet.getColumns().size(); i++) {
                ColumnDefinition col = sheet.getColumns().get(i);
                sb.append("<col min=\"").append(i + 1).append("\" max=\"").append(i + 1)
                        .append("\" width=\"").append(col.getWidth()).append("\" customWidth=\"1\"/>");
            }
            sb.append("</cols>\n");
        }

        sb.append("  <sheetData>\n");
        for (int r = 0; r < rows.size(); r++) {
            Object[] rowData = rows.get(r);
            int rowNumber = r + 1;
            sb.append("    <row r=\"").append(rowNumber).append("\">");
            for (int c = 0; c < rowData.length; c++) {
                String ref = columnName(c + 1) + rowNumber;
                sb.append(cellXml(rowData[c], ref));
            }
            sb.append("</row>\n");
        }
        sb.append("  </sheetData>\n");

        if (sheet != null && !sheet.getTables().isEmpty()) {
            sb.append("  <autoFilter ref=\"").append(escapeXml(sheet.getTables().get(0).getRange())).append("\"/>\n");
        }
        if (sheet != null && !sheet.getMerges().isEmpty()) {
            sb.append("  <mergeCells count=\"").append(sheet.getMerges().size()).append("\">");
            for (String merge : sheet.getMerges()) {
                sb.append("<mergeCell ref=\"").append(escapeXml(merge)).append("\"/>");
            }
            sb.append("</mergeCells>\n");
        }
        if (sheet != null && !sheet.getValidations().isEmpty()) {
            sb.append("  <dataValidations count=\"").append(sheet.getValidations().size()).append("\">");
            for (DataValidation dv : sheet.getValidations()) {
                sb.append("<dataValidation type=\"").append(dv.getType())
                        .append("\" sqref=\"").append(escapeXml(dv.getRange())).append("\">");
                if (dv.getFormula() != null) {
                    sb.append("<formula1>").append(escapeXml(dv.getFormula())).append("</formula1>");
                }
                sb.append("</dataValidation>");
            }
            sb.append("</dataValidations>\n");
        }

        sb.append("</worksheet>");
        return sb.toString();
    }

    private static String cellXml(Object value, String ref) {
        if (value instanceof FormulaCell) {
            FormulaCell fc = (FormulaCell) value;
            String f = fc.getFormula().startsWith("=") ? fc.getFormula().substring(1) : fc.getFormula();
            String valXml = fc.getResult() != null ? "<v>" + escapeXml(String.valueOf(fc.getResult())) + "</v>" : "";
            return "<c r=\"" + ref + "\"><f>" + escapeXml(f) + "</f>" + valXml + "</c>";
        }
        if (value == null) return "<c r=\"" + ref + "\"/>";
        if (value instanceof Number) return "<c r=\"" + ref + "\"><v>" + value + "</v></c>";
        if (value instanceof Boolean) return "<c r=\"" + ref + "\" t=\"b\"><v>" + ((Boolean) value ? 1 : 0) + "</v></c>";
        if (value instanceof Date) {
            return "<c r=\"" + ref + "\" t=\"inlineStr\"><is><t>" + escapeXml(((Date) value).toInstant().toString()) + "</t></is></c>";
        }
        return "<c r=\"" + ref + "\" t=\"inlineStr\"><is><t>" + escapeXml(String.valueOf(value)) + "</t></is></c>";
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
