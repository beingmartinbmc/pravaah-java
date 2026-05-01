package io.github.beingmartinbmc.pravaah;

import io.github.beingmartinbmc.pravaah.csv.CsvReader;
import io.github.beingmartinbmc.pravaah.csv.CsvWriter;
import io.github.beingmartinbmc.pravaah.diff.DiffEngine;
import io.github.beingmartinbmc.pravaah.formula.FormulaEngine;
import io.github.beingmartinbmc.pravaah.perf.PerfUtils;
import io.github.beingmartinbmc.pravaah.plugin.PluginRegistry;
import io.github.beingmartinbmc.pravaah.plugin.PravaahPlugin;
import io.github.beingmartinbmc.pravaah.query.QueryEngine;
import io.github.beingmartinbmc.pravaah.schema.*;
import io.github.beingmartinbmc.pravaah.xlsx.*;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Instant;
import java.time.LocalDate;
import java.util.*;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;

class PravaahTest {

    @TempDir
    Path tempDir;

    @Test
    void runtimeImplementationIsAvailable() {
        assertNotNull(Pravaah.runtimeImplementation());
        assertFalse(Pravaah.runtimeImplementation().trim().isEmpty());
    }

    // ====================== Row ======================

    @Test
    void rowOfSinglePair() {
        Row r = Row.of("k1", "v1");
        assertEquals("v1", r.get("k1"));
        assertEquals(1, r.size());
    }

    @Test
    void rowOfTwoPairs() {
        Row r = Row.of("a", 1, "b", 2);
        assertEquals(1, r.get("a"));
        assertEquals(2, r.get("b"));
    }

    @Test
    void rowOfThreePairs() {
        Row r = Row.of("a", 1, "b", 2, "c", 3);
        assertEquals(3, r.size());
    }

    @Test
    void rowOfVarargsEvenCount() {
        Row r = Row.of("a", 1, "b", 2, "c", 3, "d", 4);
        assertEquals(4, r.size());
        assertEquals(4, r.get("d"));
    }

    @Test
    void rowOfVarargsOddCountThrows() {
        assertThrows(IllegalArgumentException.class, () -> Row.of("a", 1, "b"));
    }

    @Test
    void rowCopyIsIndependent() {
        Row original = Row.of("a", 1);
        Row copy = original.copy();
        copy.put("b", 2);
        assertFalse(original.containsKey("b"));
    }

    @Test
    void rowGetStringNull() {
        Row r = Row.of("a", null);
        assertNull(r.getString("a"));
        assertNull(r.getString("missing"));
    }

    @Test
    void rowGetStringConverts() {
        Row r = Row.of("num", 42, "str", "hello");
        assertEquals("42", r.getString("num"));
        assertEquals("hello", r.getString("str"));
    }

    @Test
    void rowGetNumberFromNumber() {
        Row r = Row.of("n", 42);
        assertEquals(42, r.getNumber("n").intValue());
    }

    @Test
    void rowGetNumberFromString() {
        Row r = Row.of("n", "3.14");
        assertEquals(3.14, r.getNumber("n").doubleValue(), 0.001);
    }

    @Test
    void rowGetNumberFromBadString() {
        Row r = Row.of("n", "abc");
        assertNull(r.getNumber("n"));
    }

    @Test
    void rowGetNumberFromNull() {
        Row r = Row.of("n", null);
        assertNull(r.getNumber("n"));
    }

    @Test
    void rowGetNumberFromBoolean() {
        Row r = Row.of("n", true);
        assertNull(r.getNumber("n"));
    }

    @Test
    void rowGetBooleanFromBoolean() {
        Row r = Row.of("b", true);
        assertTrue(r.getBoolean("b"));
    }

    @Test
    void rowGetBooleanTruthyStrings() {
        for (String val : new String[]{"true", "TRUE", "1", "yes", "YES", "y", "Y"}) {
            Row r = Row.of("b", val);
            assertTrue(r.getBoolean("b"), "Expected true for: " + val);
        }
    }

    @Test
    void rowGetBooleanFalsyStrings() {
        for (String val : new String[]{"false", "FALSE", "0", "no", "NO", "n", "N"}) {
            Row r = Row.of("b", val);
            assertFalse(r.getBoolean("b"), "Expected false for: " + val);
        }
    }

    @Test
    void rowGetBooleanFromNull() {
        Row r = Row.of("b", null);
        assertNull(r.getBoolean("b"));
    }

    @Test
    void rowGetBooleanFromUnrecognized() {
        Row r = Row.of("b", "maybe");
        assertNull(r.getBoolean("b"));
    }

    @Test
    void rowGetBooleanFromNumber() {
        Row r = Row.of("b", 42);
        assertNull(r.getBoolean("b"));
    }

    @Test
    void rowDefaultConstructor() {
        Row r = new Row();
        assertTrue(r.isEmpty());
    }

    @Test
    void rowMapConstructor() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("x", 1);
        map.put("y", 2);
        Row r = new Row(map);
        assertEquals(1, r.get("x"));
        assertEquals(2, r.get("y"));
    }

    // ====================== PravaahFormat ======================

    @Test
    void formatFromCsvExtension() {
        assertEquals(PravaahFormat.CSV, PravaahFormat.fromExtension("data.csv"));
    }

    @Test
    void formatFromJsonExtension() {
        assertEquals(PravaahFormat.JSON, PravaahFormat.fromExtension("data.json"));
    }

    @Test
    void formatFromXlsxExtension() {
        assertEquals(PravaahFormat.XLSX, PravaahFormat.fromExtension("data.xlsx"));
    }

    @Test
    void formatFromXlsExtension() {
        assertEquals(PravaahFormat.XLS, PravaahFormat.fromExtension("data.xls"));
        assertEquals(PravaahFormat.XLS, PravaahFormat.fromExtension("DATA.XLS"));
    }

    @Test
    void formatFromNull() {
        assertEquals(PravaahFormat.XLSX, PravaahFormat.fromExtension(null));
    }

    @Test
    void formatFromUnknownExtension() {
        assertEquals(PravaahFormat.XLSX, PravaahFormat.fromExtension("data.txt"));
    }

    // ====================== Pipeline tests ======================

    @Test
    void mapFilterCollect() throws Exception {
        List<Row> input = Arrays.asList(
                Row.of("name", "Ada", "score", 10),
                Row.of("name", "Grace", "score", 3)
        );

        List<Row> result = Pravaah.read(input)
                .map(row -> {
                    Row r = row.copy();
                    r.put("total", ((Number) row.get("score")).intValue() * 2);
                    return r;
                })
                .filter(row -> ((Number) row.get("total")).intValue() > 10)
                .collect();

        assertEquals(1, result.size());
        assertEquals("Ada", result.get(0).get("name"));
        assertEquals(20, result.get(0).get("total"));
    }

    @Test
    void drainRows() throws Exception {
        List<Row> input = Arrays.asList(Row.of("id", 1), Row.of("id", 2));
        ProcessStats stats = Pravaah.read(input).drain();
        assertEquals(2, stats.getRowsProcessed());
        assertTrue(stats.getDurationMs() >= 0);
    }

    @Test
    void validateAndClean() throws Exception {
        List<Row> input = Collections.singletonList(
                Row.of("E-mail", " ada@example.com ", "age", "42")
        );

        List<Row> result = Pravaah.read(input)
                .clean(CleaningOptions.defaults().trim(true).fuzzyHeader("email", "E-mail"))
                .schema(SchemaDefinition.of("email", Schema.email(), "age", Schema.number()))
                .collect();

        assertEquals(1, result.size());
        assertEquals("ada@example.com", result.get(0).get("email"));
        assertEquals(42.0, result.get(0).get("age"));
    }

    @Test
    void parseDetailedValidation() throws Exception {
        byte[] csv = "email,age\nbad,old\nada@example.com,42\n".getBytes(StandardCharsets.UTF_8);
        ProcessResult result = Pravaah.parseDetailed(csv,
                SchemaDefinition.of("email", Schema.email(), "age", Schema.number()),
                ReadOptions.defaults().format(PravaahFormat.CSV).validation(ValidationMode.COLLECT));

        assertEquals(1, result.getRows().size());
        assertEquals("ada@example.com", result.getRows().get(0).get("email"));
        assertEquals(2, result.getIssues().size());
    }

    @Test
    void issueReportWriting() throws Exception {
        byte[] csv = "email,age\nbad,old\nada@example.com,42\n".getBytes(StandardCharsets.UTF_8);
        ProcessResult result = Pravaah.parseDetailed(csv,
                SchemaDefinition.of("email", Schema.email(), "age", Schema.number()),
                ReadOptions.defaults().format(PravaahFormat.CSV).validation(ValidationMode.COLLECT));

        String reportPath = tempDir.resolve("issues.csv").toString();
        SchemaValidator.writeIssueReport(result.getIssues(), reportPath);
        String reportText = new String(Files.readAllBytes(Paths.get(reportPath)));
        assertTrue(reportText.contains("invalid_type"));
    }

    // ====================== CSV tests ======================

    @Test
    void csvRoundTrip() throws Exception {
        String file = tempDir.resolve("rows.csv").toString();
        Pravaah.write(Collections.singletonList(Row.of("name", "Ada", "score", 10)), file,
                WriteOptions.defaults().format(PravaahFormat.CSV));

        List<Row> rows = Pravaah.read(file, ReadOptions.defaults().format(PravaahFormat.CSV)).collect();
        assertEquals(1, rows.size());
        assertEquals("Ada", rows.get(0).get("name"));
    }

    @Test
    void csvDrainCount() throws Exception {
        String file = tempDir.resolve("drain.csv").toString();
        List<Row> data = new ArrayList<>();
        for (int i = 0; i < 256; i++) data.add(Row.of("id", i, "name", "User " + i));
        Pravaah.write(data, file, WriteOptions.defaults().format(PravaahFormat.CSV));

        int count = CsvReader.drainCount(file, ReadOptions.defaults());
        assertEquals(256, count);
    }

    @Test
    void csvQuotedRecords() throws Exception {
        byte[] csv = "id,note\n1,\"hello\nworld\"\n2,\"escaped \"\" quote\"\n\n3,last".getBytes(StandardCharsets.UTF_8);
        int count = CsvReader.drainCount(csv, ReadOptions.defaults());
        assertEquals(3, count);
    }

    @Test
    void csvComplexRecordsMaterializeExactlyAndMatchDrainCount() throws Exception {
        byte[] csv = ("id,note,empty,tail\n"
                + "1,\"hello, world\",,\"last\"\r\n"
                + "2,\"escaped \"\"quote\"\"\",\"line\nbreak\",\n"
                + "3,plain,,").getBytes(StandardCharsets.UTF_8);

        List<Row> rows = CsvReader.readAll(csv, ReadOptions.defaults().format(PravaahFormat.CSV));

        assertRowsExactly(rows,
                Row.of("id", "1", "note", "hello, world", "empty", "", "tail", "last"),
                Row.of("id", "2", "note", "escaped \"quote\"", "empty", "line\nbreak", "tail", ""),
                Row.of("id", "3", "note", "plain", "empty", "", "tail", ""));
        assertEquals(rows.size(), CsvReader.drainCount(csv, ReadOptions.defaults()));
    }

    @Test
    void csvInferTypes() throws Exception {
        byte[] csv = "name,score,active\nAda,10,true\n".getBytes(StandardCharsets.UTF_8);
        List<Row> rows = Pravaah.read(csv, ReadOptions.defaults().format(PravaahFormat.CSV).inferTypes(true)).collect();
        assertEquals("Ada", rows.get(0).get("name"));
        assertEquals(10, rows.get(0).get("score"));
        assertEquals(true, rows.get(0).get("active"));
    }

    @Test
    void csvCrlfHandling() throws Exception {
        byte[] crlf = "a,b\r\n1,2\r\n3,4\r\n".getBytes(StandardCharsets.UTF_8);
        byte[] cr = "a,b\r1,2\r3,4\r".getBytes(StandardCharsets.UTF_8);

        List<Row> r1 = CsvReader.readAll(crlf, ReadOptions.defaults().format(PravaahFormat.CSV));
        List<Row> r2 = CsvReader.readAll(cr, ReadOptions.defaults().format(PravaahFormat.CSV));

        assertEquals(2, r1.size());
        assertEquals(2, r2.size());
    }

    @Test
    void csvEmptyFields() throws Exception {
        byte[] csv = "a,b,c\n1,,3\n,2,\n".getBytes(StandardCharsets.UTF_8);
        List<Row> rows = CsvReader.readAll(csv, ReadOptions.defaults().format(PravaahFormat.CSV));
        assertRowsExactly(rows,
                Row.of("a", "1", "b", "", "c", "3"),
                Row.of("a", "", "b", "2", "c", ""));
    }

    @Test
    void csvNoTrailingNewline() throws Exception {
        byte[] csv = "x,y\n1,2".getBytes(StandardCharsets.UTF_8);
        List<Row> rows = CsvReader.readAll(csv, ReadOptions.defaults().format(PravaahFormat.CSV));
        assertRowsExactly(rows, Row.of("x", "1", "y", "2"));
    }

    @Test
    void csvTrailingEmptyFieldWithoutNewline() throws Exception {
        byte[] csv = "a,b,c\n1,2,".getBytes(StandardCharsets.UTF_8);
        List<Row> rows = CsvReader.readAll(csv, ReadOptions.defaults().format(PravaahFormat.CSV));
        assertRowsExactly(rows, Row.of("a", "1", "b", "2", "c", ""));
    }

    @Test
    void csvSkipsEmptyRows() throws Exception {
        byte[] csv = "a,b\n\n1,2\n\n3,4\n\n".getBytes(StandardCharsets.UTF_8);
        List<Row> rows = CsvReader.readAll(csv, ReadOptions.defaults().format(PravaahFormat.CSV));
        assertEquals(2, rows.size());
    }

    @Test
    void csvExplicitHeaders() throws Exception {
        byte[] csv = "1,Ada\n2,Grace\n".getBytes(StandardCharsets.UTF_8);
        List<Row> rows = CsvReader.readAll(csv,
                ReadOptions.defaults().format(PravaahFormat.CSV).headers(false).headerNames(Arrays.asList("id", "name")));
        assertRowsExactly(rows,
                Row.of("id", "1", "name", "Ada"),
                Row.of("id", "2", "name", "Grace"));
    }

    @Test
    void csvHeaderless() throws Exception {
        byte[] csv = "1,2\n3,4\n".getBytes(StandardCharsets.UTF_8);
        List<Row> rows = CsvReader.readAll(csv, ReadOptions.defaults().format(PravaahFormat.CSV).headers(false));
        assertEquals(2, rows.size());
        assertEquals("1", rows.get(0).get("_1"));
    }

    @Test
    void csvRejectsMultiCharDelimiter() {
        assertThrows(IllegalArgumentException.class, () ->
                CsvReader.readAll("a||b\n".getBytes(StandardCharsets.UTF_8),
                        ReadOptions.defaults().delimiter("||")));
    }

    @Test
    void csvDrainRejectsMultiCharDelimiter() {
        assertThrows(IllegalArgumentException.class, () ->
                CsvReader.drainCount("a||b\n".getBytes(StandardCharsets.UTF_8),
                        ReadOptions.defaults().delimiter("||")));
    }

    @Test
    void csvDrainMalformedQuoted() {
        assertThrows(IllegalStateException.class, () ->
                CsvReader.drainCount("id,note\n1,\"unterminated".getBytes(StandardCharsets.UTF_8),
                        ReadOptions.defaults()));
        assertThrows(IllegalStateException.class, () ->
                CsvReader.drainCount("id,note\n1,\"x\"y".getBytes(StandardCharsets.UTF_8),
                        ReadOptions.defaults()));
    }

    @Test
    void csvCustomDelimiter() throws Exception {
        byte[] csv = "Ada;10;true\nGrace;;false\n".getBytes(StandardCharsets.UTF_8);
        List<Row> rows = CsvReader.readAll(csv,
                ReadOptions.defaults().format(PravaahFormat.CSV).headers(false).delimiter(";").inferTypes(true));
        assertEquals(2, rows.size());
        assertEquals("Ada", rows.get(0).get("_1"));
        assertEquals(10, rows.get(0).get("_2"));
    }

    @Test
    void csvWriteSpecialCharacters() throws Exception {
        String file = tempDir.resolve("special.csv").toString();
        List<Row> rows = Collections.singletonList(
                Row.of("name", "O'Brien, Jr.", "note", "contains \"quotes\"", "multi", "line\none"));
        Pravaah.write(rows, file, WriteOptions.defaults().format(PravaahFormat.CSV));

        String content = new String(Files.readAllBytes(Paths.get(file)));
        assertTrue(content.contains("\"O'Brien, Jr.\""));
        assertTrue(content.contains("\"\"quotes\"\""));
    }

    @Test
    void csvWriteCustomDelimiter() throws Exception {
        String file = tempDir.resolve("semi.csv").toString();
        List<Row> rows = Collections.singletonList(Row.of("a", 1, "b", 2));
        CsvWriter.write(rows, file, WriteOptions.defaults().delimiter(";"));
        String content = new String(Files.readAllBytes(Paths.get(file)));
        assertTrue(content.contains("a;b"));
        assertTrue(content.contains("1;2"));
    }

    @Test
    void csvWriteExplicitHeaders() throws Exception {
        String file = tempDir.resolve("explicit.csv").toString();
        List<Row> rows = Collections.singletonList(Row.of("a", 1, "b", 2, "c", 3));
        CsvWriter.write(rows, file, WriteOptions.defaults().headers(Arrays.asList("c", "a")));
        String content = new String(Files.readAllBytes(Paths.get(file)));
        assertTrue(content.startsWith("c,a"));
    }

    @Test
    void csvWriteNullValues() throws Exception {
        String file = tempDir.resolve("nulls.csv").toString();
        Row row = Row.of("a", 1);
        row.put("b", null);
        CsvWriter.write(Collections.singletonList(row), file, WriteOptions.defaults());
        String content = new String(Files.readAllBytes(Paths.get(file)));
        assertTrue(content.contains(","));
    }

    @Test
    void csvInferValueEdgeCases() {
        assertNull(CsvReader.inferValue(null));
        assertNull(CsvReader.inferValue(""));
        assertEquals(42, CsvReader.inferValue("42"));
        assertEquals(3.14, CsvReader.inferValue("3.14"));
        assertEquals(false, CsvReader.inferValue("FALSE"));
        assertEquals(true, CsvReader.inferValue("True"));
        assertEquals("Ada", CsvReader.inferValue("Ada"));
    }

    @Test
    void csvDrainCountHeaderless() throws Exception {
        byte[] csv = "1,2\n3,4\n".getBytes(StandardCharsets.UTF_8);
        int count = CsvReader.drainCount(csv, ReadOptions.defaults().headers(false));
        assertEquals(2, count);
    }

    @Test
    void csvDrainQuotedEndingWithDelimiter() throws Exception {
        byte[] csv = "a,b\n\"x\",y\n".getBytes(StandardCharsets.UTF_8);
        int count = CsvReader.drainCount(csv, ReadOptions.defaults());
        assertEquals(1, count);
    }

    // ====================== XLSX tests ======================

    @Test
    void xlsxRoundTrip() throws Exception {
        String file = tempDir.resolve("rows.xlsx").toString();
        Pravaah.write(Collections.singletonList(Row.of("name", "Ada", "score", 10)),
                file, WriteOptions.defaults().format(PravaahFormat.XLSX));

        List<Row> rows = Pravaah.read(file, ReadOptions.defaults().format(PravaahFormat.XLSX)).collect();
        assertEquals(1, rows.size());
        assertEquals("Ada", rows.get(0).get("name"));
        assertEquals(10, rows.get(0).get("score"));
    }

    @Test
    void xlsxMultiSheet() throws Exception {
        String file = tempDir.resolve("multi.xlsx").toString();
        Workbook wb = new Workbook(Arrays.asList(
                new Worksheet("Leads", Collections.singletonList(Row.of("name", "Ada", "score", 10))),
                new Worksheet("Finance", Collections.singletonList(Row.of("label", "Gross", "amount", 1200)))
        ));
        XlsxWriter.writeWorkbook(wb, file);

        List<Row> finance = Pravaah.read(file, ReadOptions.defaults().sheetName("Finance")).collect();
        assertEquals(1, finance.size());
        assertEquals("Gross", finance.get(0).get("label"));
        assertEquals(1200, finance.get(0).get("amount"));
    }

    @Test
    void xlsxFormulas() throws Exception {
        String file = tempDir.resolve("formulas.xlsx").toString();
        Worksheet ws = new Worksheet("Summary",
                Collections.singletonList(Row.of("label", "Total", "total", new FormulaCell("SUM(B2:B3)", 30))));
        XlsxWriter.writeWorkbook(new Workbook(Collections.singletonList(ws)), file);

        List<Row> rows = Pravaah.read(file, ReadOptions.defaults().formulas("preserve")).collect();
        assertEquals(1, rows.size());
        Object total = rows.get(0).get("total");
        assertTrue(total instanceof FormulaCell);
        assertEquals("SUM(B2:B3)", ((FormulaCell) total).getFormula());
        assertEquals(30, ((FormulaCell) total).getResult());
    }

    @Test
    void xlsxHeaderless() throws Exception {
        String file = tempDir.resolve("headerless.xlsx").toString();
        Pravaah.write(Collections.singletonList(Row.of("name", "Ada", "score", 10)),
                file, WriteOptions.defaults().format(PravaahFormat.XLSX));

        List<Row> rows = Pravaah.read(file, ReadOptions.defaults().headers(false)).collect();
        assertEquals(2, rows.size());
        assertEquals("name", rows.get(0).get("_1"));
        assertEquals("Ada", rows.get(1).get("_1"));
    }

    @Test
    void xlsxSheetNotFound() {
        String file = tempDir.resolve("notfound.xlsx").toString();
        try {
            Pravaah.write(Collections.singletonList(Row.of("id", 1)), file,
                    WriteOptions.defaults().format(PravaahFormat.XLSX));
            assertThrows(Exception.class, () ->
                    Pravaah.read(file, ReadOptions.defaults().sheetIndex(99)).collect());
        } catch (Exception e) {
            fail("Should not throw during write");
        }
    }

    @Test
    void xlsxBySheetIndex() throws Exception {
        String file = tempDir.resolve("indexed.xlsx").toString();
        Workbook wb = new Workbook(Arrays.asList(
                new Worksheet("Sheet1", Collections.singletonList(Row.of("v", "first"))),
                new Worksheet("Sheet2", Collections.singletonList(Row.of("v", "second")))
        ));
        XlsxWriter.writeWorkbook(wb, file);

        List<Row> rows = Pravaah.read(file, ReadOptions.defaults().sheetIndex(1)).collect();
        assertEquals(1, rows.size());
        assertEquals("second", rows.get(0).get("v"));
    }

    @Test
    void xlsxMultipleRows() throws Exception {
        String file = tempDir.resolve("multi_rows.xlsx").toString();
        List<Row> data = Arrays.asList(
                Row.of("id", 1, "name", "Ada"),
                Row.of("id", 2, "name", "Grace"),
                Row.of("id", 3, "name", "Linus")
        );
        Pravaah.write(data, file, WriteOptions.defaults().format(PravaahFormat.XLSX));
        List<Row> rows = Pravaah.read(file, ReadOptions.defaults().format(PravaahFormat.XLSX)).collect();
        assertEquals(3, rows.size());
    }

    // ====================== XLS tests ======================

    @Test
    void xlsReadsDefaultSheetWithStringsNumbersBooleansBlanksAndFormulaValues() throws Exception {
        String file = tempDir.resolve("legacy.xls").toString();
        Files.write(Paths.get(file), sampleXlsWorkbook());

        List<Row> rows = Pravaah.read(file).collect();

        assertRowsExactly(rows,
                Row.of("email", "ada@example.com", "score", 42, "active", true,
                        "note", "hello, world", "blank", null, "computed", 84),
                Row.of("email", "grace@example.com", "score", 7.5, "active", false,
                        "note", "escaped \"quote\"", "blank", null, "computed", 15));
    }

    @Test
    void xlsSupportsSheetNameSheetIndexHeaderlessAndExplicitHeaders() throws Exception {
        byte[] workbook = sampleXlsWorkbook();

        List<Row> byName = Pravaah.read(workbook,
                ReadOptions.defaults().format(PravaahFormat.XLS).sheetName("Second")).collect();
        assertRowsExactly(byName, Row.of("kind", "secondary", "value", 99));

        List<Row> byIndex = Pravaah.read(workbook,
                ReadOptions.defaults().format(PravaahFormat.XLS).sheetIndex(1)).collect();
        assertEquals(byName, byIndex);

        List<Row> headerless = Pravaah.read(workbook,
                ReadOptions.defaults().format(PravaahFormat.XLS).headers(false)).collect();
        assertRowsExactly(headerless,
                Row.of("_1", "email", "_2", "score", "_3", "active", "_4", "note", "_5", "blank", "_6", "computed"),
                Row.of("_1", "ada@example.com", "_2", 42, "_3", true,
                        "_4", "hello, world", "_5", null, "_6", 84),
                Row.of("_1", "grace@example.com", "_2", 7.5, "_3", false,
                        "_4", "escaped \"quote\"", "_5", null, "_6", 15));

        List<Row> explicit = Pravaah.read(workbook,
                ReadOptions.defaults().format(PravaahFormat.XLS)
                        .headers(false)
                        .headerNames(Arrays.asList("col1", "col2"))).collect();
        assertRowsExactly(explicit,
                Row.of("col1", "email", "col2", "score"),
                Row.of("col1", "ada@example.com", "col2", 42),
                Row.of("col1", "grace@example.com", "col2", 7.5));
    }

    @Test
    void xlsValidationAndCorruptWorkbookFailuresAreClear() throws Exception {
        ProcessResult result = Pravaah.parseDetailed(sampleXlsWorkbook(),
                SchemaDefinition.of("email", Schema.email(), "score", Schema.number(), "active", Schema.bool()),
                ReadOptions.defaults().format(PravaahFormat.XLS).validation(ValidationMode.COLLECT));

        assertEquals(2, result.getRows().size());
        assertTrue(result.getIssues().isEmpty());

        assertThrows(Exception.class, () ->
                Pravaah.read("not an xls".getBytes(StandardCharsets.UTF_8),
                        ReadOptions.defaults().format(PravaahFormat.XLS)).collect());
    }

    @Test
    void csvXlsAndXlsxReturnEquivalentRowsForIngestionData() throws Exception {
        List<Row> expected = Arrays.asList(
                Row.of("email", "ada@example.com", "score", 42, "active", true,
                        "note", "hello, world", "blank", null, "computed", 84),
                Row.of("email", "grace@example.com", "score", 7.5, "active", false,
                        "note", "escaped \"quote\"", "blank", null, "computed", 15)
        );

        String csvFile = tempDir.resolve("parity.csv").toString();
        String xlsxFile = tempDir.resolve("parity.xlsx").toString();
        String xlsFile = tempDir.resolve("parity.xls").toString();
        Pravaah.write(expected, csvFile, WriteOptions.defaults().format(PravaahFormat.CSV));
        Pravaah.write(expected, xlsxFile, WriteOptions.defaults().format(PravaahFormat.XLSX));
        Files.write(Paths.get(xlsFile), sampleXlsWorkbook());

        List<Row> csv = Pravaah.read(csvFile, ReadOptions.defaults().format(PravaahFormat.CSV).inferTypes(true)).collect();
        List<Row> xlsx = Pravaah.read(xlsxFile, ReadOptions.defaults().format(PravaahFormat.XLSX)).collect();
        List<Row> xls = Pravaah.read(xlsFile, ReadOptions.defaults().format(PravaahFormat.XLS)).collect();

        assertEquals(expected, csv);
        assertEquals(expected, xlsx);
        assertEquals(expected, xls);
    }

    // ====================== Schema tests ======================

    @Test
    void schemaCleaningAndValidation() {
        List<Row> input = Arrays.asList(
                Row.of("Email Address", " ada@example.com ", "id", "1", "active", "yes"),
                Row.of("Email Address", " ada@example.com ", "id", "1", "active", "no")
        );

        List<Row> cleaned = SchemaValidator.cleanRows(input, CleaningOptions.defaults()
                .trim(true).normalizeWhitespace(true)
                .dedupeKey("id", "Email Address")
                .fuzzyHeader("email", "email address"));

        assertEquals("email id", SchemaValidator.normalizeHeader(" Email_ID "));
        assertEquals(1, cleaned.size());
        assertEquals("ada@example.com", cleaned.get(0).get("email"));
    }

    @Test
    void schemaValidatesDefaults() {
        List<Row> cleaned = Collections.singletonList(
                Row.of("id", "1", "email", "ada@example.com", "active", "yes",
                        "phone", "(555) 123-4567")
        );

        SchemaDefinition def = new SchemaDefinition()
                .field("id", Schema.number())
                .field("email", Schema.email())
                .field("active", Schema.bool())
                .field("phone", Schema.phone())
                .field("role", Schema.string().defaultValue("lead"))
                .field("optional", Schema.string(true))
                .field("raw", Schema.any().defaultValue(Collections.singletonMap("source", "test")));

        ProcessResult result = SchemaValidator.validateRows(cleaned, def, ValidationMode.COLLECT, null);
        assertTrue(result.getIssues().isEmpty());
        assertEquals(1.0, result.getRows().get(0).get("id"));
        assertEquals("ada@example.com", result.getRows().get(0).get("email"));
        assertEquals(true, result.getRows().get(0).get("active"));
        assertEquals("lead", result.getRows().get(0).get("role"));
        assertNull(result.getRows().get(0).get("optional"));
    }

    @Test
    void schemaFailFast() {
        assertThrows(PravaahValidationException.class, () ->
                SchemaValidator.validateRows(
                        Collections.singletonList(Row.of("id", "bad")),
                        SchemaDefinition.of("id", Schema.number()),
                        ValidationMode.FAIL_FAST, null));
    }

    @Test
    void schemaBooleanFalseCoercion() {
        ProcessResult result = SchemaValidator.validateRows(
                Arrays.asList(Row.of("flag", "0"), Row.of("flag", "n"), Row.of("flag", "no")),
                SchemaDefinition.of("flag", Schema.bool()),
                ValidationMode.COLLECT, null);
        assertEquals(3, result.getRows().size());
        assertEquals(false, result.getRows().get(0).get("flag"));
        assertEquals(false, result.getRows().get(1).get("flag"));
        assertEquals(false, result.getRows().get(2).get("flag"));
    }

    @Test
    void schemaSkipMode() {
        ProcessResult result = SchemaValidator.validateRows(
                Arrays.asList(Row.of("id", "1"), Row.of("id", "bad"), Row.of("id", "3")),
                SchemaDefinition.of("id", Schema.number()),
                ValidationMode.SKIP, null);
        assertEquals(2, result.getRows().size());
        assertTrue(result.getIssues().isEmpty());
    }

    @Test
    void schemaCoerceFalse() {
        SchemaDefinition def = SchemaDefinition.of(
                "id", Schema.number().coerce(false),
                "active", Schema.bool().coerce(false));

        ProcessResult result = SchemaValidator.validateRows(
                Collections.singletonList(Row.of("id", "1", "active", "yes")),
                def, ValidationMode.COLLECT, null);
        assertEquals(0, result.getRows().size());
        assertEquals(2, result.getIssues().size());
    }

    @Test
    void schemaDateCoercion() {
        SchemaDefinition def = SchemaDefinition.of("d", Schema.date());
        ProcessResult result = SchemaValidator.validateRows(
                Collections.singletonList(Row.of("d", "2024-06-15")),
                def, ValidationMode.COLLECT, null);
        assertEquals(1, result.getRows().size());
        assertEquals(LocalDate.of(2024, 6, 15), result.getRows().get(0).get("d"));
    }

    @Test
    void schemaDateFromInstantString() {
        SchemaDefinition def = SchemaDefinition.of("d", Schema.date());
        ProcessResult result = SchemaValidator.validateRows(
                Collections.singletonList(Row.of("d", "2024-06-15T10:30:00Z")),
                def, ValidationMode.COLLECT, null);
        assertEquals(1, result.getRows().size());
        assertTrue(result.getRows().get(0).get("d") instanceof Instant);
    }

    @Test
    void schemaDateInvalidString() {
        SchemaDefinition def = SchemaDefinition.of("d", Schema.date());
        ProcessResult result = SchemaValidator.validateRows(
                Collections.singletonList(Row.of("d", "not-a-date")),
                def, ValidationMode.COLLECT, null);
        assertEquals(0, result.getRows().size());
        assertEquals(1, result.getIssues().size());
    }

    @Test
    void schemaDateNativeObjects() {
        LocalDate ld = LocalDate.of(2024, 1, 1);
        Instant inst = Instant.now();
        SchemaDefinition def = SchemaDefinition.of("d", Schema.date());

        ProcessResult r1 = SchemaValidator.validateRows(
                Collections.singletonList(Row.of("d", ld)), def, ValidationMode.COLLECT, null);
        assertEquals(ld, r1.getRows().get(0).get("d"));

        ProcessResult r2 = SchemaValidator.validateRows(
                Collections.singletonList(Row.of("d", inst)), def, ValidationMode.COLLECT, null);
        assertEquals(inst, r2.getRows().get(0).get("d"));
    }

    @Test
    void schemaDateCoerceFalseWithString() {
        SchemaDefinition def = SchemaDefinition.of("d", Schema.date().coerce(false));
        ProcessResult result = SchemaValidator.validateRows(
                Collections.singletonList(Row.of("d", "2024-01-01")),
                def, ValidationMode.COLLECT, null);
        assertEquals(0, result.getRows().size());
        assertEquals(1, result.getIssues().size());
    }

    @Test
    void schemaPhoneTooShort() {
        ProcessResult result = SchemaValidator.validateRows(
                Collections.singletonList(Row.of("p", "123")),
                SchemaDefinition.of("p", Schema.phone()),
                ValidationMode.COLLECT, null);
        assertEquals(0, result.getRows().size());
        assertEquals(1, result.getIssues().size());
    }

    @Test
    void schemaMissingRequiredField() {
        ProcessResult result = SchemaValidator.validateRows(
                Collections.singletonList(Row.of("other", "value")),
                SchemaDefinition.of("id", Schema.number()),
                ValidationMode.COLLECT, null);
        assertEquals(0, result.getRows().size());
        assertTrue(result.getIssues().get(0).getCode().equals("missing_column"));
    }

    @Test
    void schemaMissingFieldWithEmptyString() {
        ProcessResult result = SchemaValidator.validateRows(
                Collections.singletonList(Row.of("id", "")),
                SchemaDefinition.of("id", Schema.number()),
                ValidationMode.COLLECT, null);
        assertEquals(0, result.getRows().size());
        assertEquals("missing_column", result.getIssues().get(0).getCode());
    }

    @Test
    void schemaCustomValidateFunction() {
        FieldDefinition fd = Schema.number().validate((value, row) -> {
            double d = ((Number) value).doubleValue();
            return d > 100 ? "value too large" : null;
        });

        ProcessResult r1 = SchemaValidator.validateRows(
                Collections.singletonList(Row.of("n", "50")),
                SchemaDefinition.of("n", fd), ValidationMode.COLLECT, null);
        assertEquals(1, r1.getRows().size());
        assertTrue(r1.getIssues().isEmpty());

        ProcessResult r2 = SchemaValidator.validateRows(
                Collections.singletonList(Row.of("n", "200")),
                SchemaDefinition.of("n", fd), ValidationMode.COLLECT, null);
        assertEquals(0, r2.getRows().size());
        assertEquals("invalid_value", r2.getIssues().get(0).getCode());
    }

    @Test
    void schemaAnyKind() {
        ProcessResult result = SchemaValidator.validateRows(
                Collections.singletonList(Row.of("a", "anything", "b", 42)),
                new SchemaDefinition().field("a", Schema.any()).field("b", Schema.any()),
                ValidationMode.COLLECT, null);
        assertEquals(1, result.getRows().size());
        assertEquals("anything", result.getRows().get(0).get("a"));
        assertEquals(42, result.getRows().get(0).get("b"));
    }

    @Test
    void schemaStringKindCoercion() {
        ProcessResult result = SchemaValidator.validateRows(
                Collections.singletonList(Row.of("s", 42)),
                SchemaDefinition.of("s", Schema.string()),
                ValidationMode.COLLECT, null);
        assertEquals("42", result.getRows().get(0).get("s"));
    }

    @Test
    void schemaNumberAlreadyNumber() {
        ProcessResult result = SchemaValidator.validateRows(
                Collections.singletonList(Row.of("n", 42)),
                SchemaDefinition.of("n", Schema.number()),
                ValidationMode.COLLECT, null);
        assertEquals(42.0, result.getRows().get(0).get("n"));
    }

    @Test
    void schemaBooleanAlreadyBoolean() {
        ProcessResult result = SchemaValidator.validateRows(
                Collections.singletonList(Row.of("b", true)),
                SchemaDefinition.of("b", Schema.bool()),
                ValidationMode.COLLECT, null);
        assertEquals(true, result.getRows().get(0).get("b"));
    }

    @Test
    void schemaEmailInvalid() {
        ProcessResult result = SchemaValidator.validateRows(
                Collections.singletonList(Row.of("e", "not-an-email")),
                SchemaDefinition.of("e", Schema.email()),
                ValidationMode.COLLECT, null);
        assertEquals(0, result.getRows().size());
        assertEquals(1, result.getIssues().size());
    }

    @Test
    void schemaBooleanUnrecognized() {
        ProcessResult result = SchemaValidator.validateRows(
                Collections.singletonList(Row.of("b", "maybe")),
                SchemaDefinition.of("b", Schema.bool()),
                ValidationMode.COLLECT, null);
        assertEquals(0, result.getRows().size());
        assertEquals(1, result.getIssues().size());
    }

    @Test
    void schemaNumberNaN() {
        ProcessResult result = SchemaValidator.validateRows(
                Collections.singletonList(Row.of("n", "NaN")),
                SchemaDefinition.of("n", Schema.number()),
                ValidationMode.COLLECT, null);
        assertEquals(0, result.getRows().size());
    }

    @Test
    void schemaOptionalWithOptionalMethod() {
        FieldDefinition fd = Schema.number(true);
        assertTrue(fd.isOptional());
    }

    @Test
    void schemaCleanRowNullOptions() {
        Row row = Row.of("a", "val");
        Row result = SchemaValidator.cleanRow(row, null);
        assertSame(row, result);
    }

    @Test
    void schemaNormalizeWhitespace() {
        List<Row> input = Collections.singletonList(Row.of("name", "  John   Doe  "));
        List<Row> cleaned = SchemaValidator.cleanRows(input,
                CleaningOptions.defaults().trim(true).normalizeWhitespace(true));
        assertEquals("John Doe", cleaned.get(0).get("name"));
    }

    @Test
    void schemaFuzzyHeadersAlreadyPresent() {
        Row row = Row.of("email", "ada@example.com", "Email Address", "old@example.com");
        Row result = SchemaValidator.applyFuzzyHeaders(row,
                Collections.singletonMap("email", Collections.singletonList("email address")));
        assertEquals("ada@example.com", result.get("email"));
    }

    @Test
    void schemaFuzzyHeadersEmptyAliases() {
        Row row = Row.of("a", 1);
        Row result = SchemaValidator.applyFuzzyHeaders(row, null);
        assertSame(row, result);
        Row result2 = SchemaValidator.applyFuzzyHeaders(row, new LinkedHashMap<>());
        assertSame(row, result2);
    }

    @Test
    void schemaIsDuplicateNullKey() {
        Set<String> seen = new HashSet<>();
        assertFalse(SchemaValidator.isDuplicate(Row.of("a", 1), null, seen));
    }

    @Test
    void schemaValidateRowsWithCleaning() {
        ProcessResult result = SchemaValidator.validateRows(
                Collections.singletonList(Row.of("id", " 42 ")),
                SchemaDefinition.of("id", Schema.number()),
                ValidationMode.COLLECT,
                CleaningOptions.defaults().trim(true));
        assertEquals(1, result.getRows().size());
        assertEquals(42.0, result.getRows().get(0).get("id"));
    }

    @Test
    void schemaCleanRowsNoDedupeKey() {
        List<Row> input = Arrays.asList(Row.of("a", 1), Row.of("a", 1));
        List<Row> cleaned = SchemaValidator.cleanRows(input, CleaningOptions.defaults());
        assertEquals(2, cleaned.size());
    }

    @Test
    void schemaCleanRowsDeduplicates() {
        List<Row> input = Arrays.asList(
                Row.of("id", "1", "v", "a"),
                Row.of("id", "1", "v", "b"),
                Row.of("id", "2", "v", "c"));
        List<Row> cleaned = SchemaValidator.cleanRows(input, CleaningOptions.defaults().dedupeKey("id"));
        assertEquals(2, cleaned.size());
    }

    @Test
    void schemaCleanNonStringValues() {
        List<Row> input = Collections.singletonList(Row.of("num", 42, "bool", true));
        List<Row> cleaned = SchemaValidator.cleanRows(input, CleaningOptions.defaults().trim(true));
        assertEquals(42, cleaned.get(0).get("num"));
        assertEquals(true, cleaned.get(0).get("bool"));
    }

    // ====================== Formula tests ======================

    @Test
    void formulaEvaluation() {
        assertEquals(10.0, FormulaEngine.evaluateFormula("=SUM(a,b,5)", Row.of("a", 2, "b", 3)));
        assertEquals("yes", FormulaEngine.evaluateFormula("=IF(active,\"yes\",\"no\")", Row.of("active", true)));
    }

    @Test
    void formulaFunctions() {
        FormulaEngine engine = new FormulaEngine();
        assertEquals(4.0, engine.evaluate("AVERAGE(2,4,6)"));
        assertEquals(2.0, engine.evaluate("MIN(2,4,6)"));
        assertEquals(6.0, engine.evaluate("MAX(2,4,6)"));
        assertEquals(2, engine.evaluate("COUNT(1,\"x\",2)"));
        assertEquals("no", engine.evaluate("IF(false,\"yes\",\"no\")"));
    }

    @Test
    void formulaCustomFunctions() {
        Map<String, FormulaEngine.FormulaFunction> custom = new LinkedHashMap<>();
        custom.put("DOUBLE", (args, row) -> ((Number) args.get(0)).doubleValue() * 2);
        FormulaEngine engine = new FormulaEngine(custom);
        assertEquals(10.0, engine.evaluate("DOUBLE(score)", Row.of("score", 5)));
    }

    @Test
    void formulaExpressions() {
        FormulaEngine engine = new FormulaEngine();
        assertEquals(18.0, engine.evaluate("score * (bonus + 2)", Row.of("score", 3, "bonus", 4)));
    }

    @Test
    void formulaUnsupported() {
        FormulaEngine engine = new FormulaEngine();
        assertThrows(IllegalArgumentException.class, () -> engine.evaluate("MISSING(1)"));
    }

    @Test
    void formulaConcat() {
        FormulaEngine engine = new FormulaEngine();
        assertEquals("helloworld", engine.evaluate("CONCAT(\"hello\",\"world\")"));
    }

    @Test
    void formulaConcatWithRow() {
        FormulaEngine engine = new FormulaEngine();
        assertEquals("Ada10", engine.evaluate("CONCAT(name,score)", Row.of("name", "Ada", "score", 10)));
    }

    @Test
    void formulaArithmeticOperations() {
        FormulaEngine engine = new FormulaEngine();
        assertEquals(5.0, engine.evaluate("10 / 2", new Row()));
        assertEquals(7.0, engine.evaluate("3 + 4", new Row()));
        assertEquals(6.0, engine.evaluate("2 * 3", new Row()));
        assertEquals(1.0, engine.evaluate("5 - 4", new Row()));
    }

    @Test
    void formulaArithmeticNegative() {
        FormulaEngine engine = new FormulaEngine();
        assertEquals(-3.0, engine.evaluate("-3", new Row()));
    }

    @Test
    void formulaArithmeticParentheses() {
        FormulaEngine engine = new FormulaEngine();
        assertEquals(14.0, engine.evaluate("(3 + 4) * 2", new Row()));
    }

    @Test
    void formulaStaticNoRow() {
        Object result = FormulaEngine.evaluateFormula("SUM(1,2,3)");
        assertEquals(6.0, result);
    }

    @Test
    void formulaLeadingEquals() {
        FormulaEngine engine = new FormulaEngine();
        assertEquals(6.0, engine.evaluate("=SUM(1,2,3)"));
    }

    @Test
    void formulaIfTruthyEdgeCases() {
        FormulaEngine engine = new FormulaEngine();
        assertEquals("yes", engine.evaluate("IF(1,\"yes\",\"no\")"));
        assertEquals("no", engine.evaluate("IF(0,\"yes\",\"no\")"));
        assertEquals("yes", engine.evaluate("IF(\"nonempty\",\"yes\",\"no\")"));
    }

    @Test
    void formulaAverageEmpty() {
        FormulaEngine engine = new FormulaEngine();
        assertEquals(0.0, engine.evaluate("AVERAGE(\"x\")"));
    }

    @Test
    void formulaNullCustomFunctions() {
        FormulaEngine engine = new FormulaEngine(null);
        assertEquals(6.0, engine.evaluate("SUM(1,2,3)"));
    }

    @Test
    void formulaRegister() {
        FormulaEngine engine = new FormulaEngine();
        engine.register("TRIPLE", (args, row) -> ((Number) args.get(0)).doubleValue() * 3);
        assertEquals(15.0, engine.evaluate("TRIPLE(5)"));
    }

    @Test
    void formulaNestedFunctionCalls() {
        FormulaEngine engine = new FormulaEngine();
        engine.register("ADD", (args, row) -> {
            double a = ((Number) args.get(0)).doubleValue();
            double b = ((Number) args.get(1)).doubleValue();
            return a + b;
        });
        assertEquals(6.0, engine.evaluate("SUM(3,3)"));
    }

    @Test
    void formulaExpressionFallback() {
        FormulaEngine engine = new FormulaEngine();
        Object result = engine.evaluate("hello", new Row());
        assertEquals("hello", result);
    }

    // ====================== Query tests ======================

    @Test
    void queryBasic() {
        List<Row> rows = Arrays.asList(
                Row.of("id", 1, "name", "Ada", "score", 10),
                Row.of("id", 2, "name", "Grace", "score", 3)
        );

        List<Row> result = QueryEngine.query(rows, "SELECT name, score WHERE score > 2 ORDER BY score DESC LIMIT 1");
        assertEquals(1, result.size());
        assertEquals("Ada", result.get(0).get("name"));
    }

    @Test
    void queryOperators() {
        List<Row> rows = Arrays.asList(
                Row.of("id", 1, "name", "Ada", "score", 10),
                Row.of("id", 2, "name", "Grace", "score", 3),
                Row.of("id", 3, "name", "Linus", "score", 8)
        );

        assertEquals(2, QueryEngine.query(rows, "SELECT name WHERE score >= 8").size());
        assertEquals(1, QueryEngine.query(rows, "SELECT name WHERE score < 8").size());
        assertEquals(2, QueryEngine.query(rows, "SELECT name WHERE score != 3").size());
    }

    @Test
    void queryContains() {
        List<Row> rows = Arrays.asList(
                Row.of("name", "Ada"), Row.of("name", "Grace")
        );
        assertEquals(2, QueryEngine.query(rows, "SELECT * WHERE name contains 'a'").size());
    }

    @Test
    void queryNullSource() {
        assertThrows(IllegalArgumentException.class, () -> QueryEngine.query(null, "SELECT *"));
    }

    @Test
    void queryBadSql() {
        assertThrows(IllegalArgumentException.class, () -> QueryEngine.query(new ArrayList<Row>(), "bad sql"));
    }

    @Test
    void indexAndJoin() {
        List<Row> rows = Arrays.asList(
                Row.of("team", "a", "score", 10),
                Row.of("team", "a", "score", 3)
        );
        Map<String, List<Row>> index = QueryEngine.createIndex(rows, "team", "score");
        assertEquals(1, index.get("a\u000010").size());

        List<Row> left = Collections.singletonList(Row.of("id", 1, "left", true));
        List<Row> right = Arrays.asList(Row.of("id", 1, "right", true), Row.of("id", 2, "right", false));
        List<Row> joined = QueryEngine.joinRows(left, right, "id");
        assertEquals(1, joined.size());
        assertEquals(true, joined.get(0).get("left"));
        assertEquals(true, joined.get(0).get("right"));
    }

    @Test
    void queryEqualsOperator() {
        List<Row> rows = Arrays.asList(
                Row.of("name", "Ada", "score", 10),
                Row.of("name", "Grace", "score", 3)
        );
        List<Row> result = QueryEngine.query(rows, "SELECT * WHERE score = 10");
        assertEquals(1, result.size());
        assertEquals("Ada", result.get(0).get("name"));
    }

    @Test
    void queryLessOrEqual() {
        List<Row> rows = Arrays.asList(
                Row.of("score", 10), Row.of("score", 5), Row.of("score", 3));
        assertEquals(2, QueryEngine.query(rows, "SELECT * WHERE score <= 5").size());
    }

    @Test
    void queryAscOrder() {
        List<Row> rows = Arrays.asList(
                Row.of("name", "Grace", "score", 3),
                Row.of("name", "Ada", "score", 10)
        );
        List<Row> result = QueryEngine.query(rows, "SELECT * ORDER BY score ASC");
        assertEquals("Grace", result.get(0).get("name"));
    }

    @Test
    void querySelectAll() {
        List<Row> rows = Arrays.asList(Row.of("a", 1, "b", 2));
        List<Row> result = QueryEngine.query(rows, "SELECT *");
        assertEquals(1, result.size());
        assertEquals(1, result.get(0).get("a"));
        assertEquals(2, result.get(0).get("b"));
    }

    @Test
    void queryLimitWithoutOrderBy() {
        List<Row> rows = new ArrayList<>();
        for (int i = 0; i < 10; i++) rows.add(Row.of("id", i));
        List<Row> result = QueryEngine.query(rows, "SELECT * LIMIT 3");
        assertEquals(3, result.size());
    }

    @Test
    void queryProjectColumns() {
        List<Row> rows = Collections.singletonList(Row.of("a", 1, "b", 2, "c", 3));
        List<Row> result = QueryEngine.query(rows, "SELECT a, c");
        assertEquals(1, result.get(0).get("a"));
        assertNull(result.get(0).get("b"));
        assertEquals(3, result.get(0).get("c"));
    }

    @Test
    void queryOrderByString() {
        List<Row> rows = Arrays.asList(
                Row.of("name", "Charlie"),
                Row.of("name", "Alice"),
                Row.of("name", "Bob")
        );
        List<Row> result = QueryEngine.query(rows, "SELECT * ORDER BY name ASC");
        assertEquals("Alice", result.get(0).get("name"));
    }

    @Test
    void queryContainsWithNullValue() {
        List<Row> rows = Arrays.asList(Row.of("name", "Ada"), Row.of("x", 1));
        List<Row> result = QueryEngine.query(rows, "SELECT * WHERE name contains 'Ad'");
        assertEquals(1, result.size());
    }

    @Test
    void queryWhereNonNumericComparison() {
        List<Row> rows = Collections.singletonList(Row.of("name", "Ada"));
        List<Row> result = QueryEngine.query(rows, "SELECT * WHERE name > 5");
        assertEquals(0, result.size());
    }

    @Test
    void joinRowsNoMatch() {
        List<Row> left = Collections.singletonList(Row.of("id", 1));
        List<Row> right = Collections.singletonList(Row.of("id", 2));
        List<Row> joined = QueryEngine.joinRows(left, right, "id");
        assertEquals(0, joined.size());
    }

    @Test
    void createIndexSingleKey() {
        List<Row> rows = Arrays.asList(
                Row.of("cat", "a", "v", 1),
                Row.of("cat", "a", "v", 2),
                Row.of("cat", "b", "v", 3)
        );
        Map<String, List<Row>> index = QueryEngine.createIndex(rows, "cat");
        assertEquals(2, index.get("a").size());
        assertEquals(1, index.get("b").size());
    }

    // ====================== Diff tests ======================

    @Test
    void diffBasic() {
        List<Row> oldRows = Arrays.asList(
                Row.of("id", 1, "name", "Ada", "score", 10),
                Row.of("id", 2, "name", "Grace", "score", 3)
        );
        List<Row> newRows = Collections.singletonList(Row.of("id", 1, "name", "Ada", "score", 11));

        DiffEngine.DiffResult result = DiffEngine.diff(oldRows, newRows, "id");
        assertEquals(0, result.getAdded().size());
        assertEquals(1, result.getRemoved().size());
        assertEquals(1, result.getChanged().size());
        assertEquals(0, result.getUnchanged());
    }

    @Test
    void diffReport() throws Exception {
        List<Row> oldRows = Arrays.asList(Row.of("id", 1, "v", 1), Row.of("id", 2, "v", 2));
        List<Row> newRows = Arrays.asList(Row.of("id", 1, "v", 3), Row.of("id", 3, "v", 4));

        DiffEngine.DiffResult result = DiffEngine.diff(oldRows, newRows, "id");
        String report = tempDir.resolve("diff.csv").toString();
        DiffEngine.writeDiffReport(result, report);
        String text = new String(Files.readAllBytes(Paths.get(report)));
        assertTrue(text.contains("changed"));
        assertTrue(text.contains("added"));
    }

    @Test
    void diffUnchanged() {
        List<Row> rows = Arrays.asList(Row.of("id", 1, "v", "same"), Row.of("id", 2, "v", "same2"));
        DiffEngine.DiffResult result = DiffEngine.diff(rows, rows, "id");
        assertEquals(0, result.getAdded().size());
        assertEquals(0, result.getRemoved().size());
        assertEquals(0, result.getChanged().size());
        assertEquals(2, result.getUnchanged());
    }

    @Test
    void diffAdded() {
        List<Row> old = Collections.emptyList();
        List<Row> newRows = Arrays.asList(Row.of("id", 1), Row.of("id", 2));
        DiffEngine.DiffResult result = DiffEngine.diff(old, newRows, "id");
        assertEquals(2, result.getAdded().size());
        assertEquals(0, result.getRemoved().size());
    }

    @Test
    void diffRemoved() {
        List<Row> old = Arrays.asList(Row.of("id", 1), Row.of("id", 2));
        List<Row> newRows = Collections.emptyList();
        DiffEngine.DiffResult result = DiffEngine.diff(old, newRows, "id");
        assertEquals(0, result.getAdded().size());
        assertEquals(2, result.getRemoved().size());
    }

    @Test
    void diffChangedColumnDetails() {
        List<Row> old = Collections.singletonList(Row.of("id", 1, "v", "old", "x", "same"));
        List<Row> newRows = Collections.singletonList(Row.of("id", 1, "v", "new", "x", "same"));
        DiffEngine.DiffResult result = DiffEngine.diff(old, newRows, "id");
        assertEquals(1, result.getChanged().size());
        DiffEngine.RowChange change = result.getChanged().get(0);
        assertTrue(change.getChangedColumns().contains("v"));
        assertFalse(change.getChangedColumns().contains("x"));
        assertNotNull(change.getBefore());
        assertNotNull(change.getAfter());
        assertNotNull(change.getKey());
    }

    @Test
    void diffMultiKey() {
        List<Row> old = Collections.singletonList(Row.of("a", 1, "b", 2, "v", "old"));
        List<Row> newRows = Collections.singletonList(Row.of("a", 1, "b", 2, "v", "new"));
        DiffEngine.DiffResult result = DiffEngine.diff(old, newRows, "a", "b");
        assertEquals(1, result.getChanged().size());
    }

    @Test
    void diffConvenienceMethod() {
        List<Row> old = Collections.singletonList(Row.of("id", 1, "v", 1));
        List<Row> newRows = Collections.singletonList(Row.of("id", 1, "v", 2));
        DiffEngine.DiffResult result = Pravaah.diff(old, newRows, "id");
        assertEquals(1, result.getChanged().size());
    }

    @Test
    void diffReportEmptyResult() throws Exception {
        List<Row> rows = Collections.singletonList(Row.of("id", 1, "v", 1));
        DiffEngine.DiffResult result = DiffEngine.diff(rows, rows, "id");
        String report = tempDir.resolve("empty_diff.csv").toString();
        DiffEngine.writeDiffReport(result, report);
        String text = new String(Files.readAllBytes(Paths.get(report)));
        assertTrue(text.contains("type,key,changedColumns,before,after"));
    }

    // ====================== Plugin tests ======================

    @Test
    void pluginRegistry() {
        PluginRegistry registry = new PluginRegistry();

        Map<String, Function<List<Object>, Object>> formulas = new LinkedHashMap<>();
        formulas.put("SCORE", args -> ((Number) args.get(0)).doubleValue() + 1);

        List<java.util.function.Function<Row, List<PravaahIssue>>> validators = new ArrayList<>();
        validators.add(row -> {
            Object valid = row.get("valid");
            if (Boolean.TRUE.equals(valid)) return Collections.emptyList();
            return Collections.singletonList(PravaahIssue.error("invalid", "invalid row", 1, null, null, null));
        });

        registry.use(new PravaahPlugin("quality").formulas(formulas).validators(validators));

        assertThrows(IllegalStateException.class, () -> registry.use(new PravaahPlugin("quality")));
        assertEquals(1, registry.list().size());
        assertEquals(2.0, registry.formulas().get("SCORE").apply(Collections.<Object>singletonList(1)));
        assertEquals(1, registry.validate(Row.of("valid", false)).size());
        assertEquals(1, registry.validateRows(Arrays.asList(Row.of("valid", true), Row.of("valid", false))).size());
    }

    @Test
    void pluginEmptyFormulasAndValidators() {
        PluginRegistry registry = new PluginRegistry();
        registry.use(new PravaahPlugin("minimal"));
        assertEquals(1, registry.list().size());
        assertTrue(registry.formulas().isEmpty());
        assertTrue(registry.validators().isEmpty());
        assertTrue(registry.validate(Row.of("a", 1)).isEmpty());
    }

    @Test
    void pluginMultiplePlugins() {
        PluginRegistry registry = new PluginRegistry();

        Map<String, Function<List<Object>, Object>> f1 = new LinkedHashMap<>();
        f1.put("FN1", args -> 1);
        Map<String, Function<List<Object>, Object>> f2 = new LinkedHashMap<>();
        f2.put("FN2", args -> 2);

        registry.use(new PravaahPlugin("p1").formulas(f1));
        registry.use(new PravaahPlugin("p2").formulas(f2));

        assertEquals(2, registry.list().size());
        assertEquals(2, registry.formulas().size());
        assertTrue(registry.formulas().containsKey("FN1"));
        assertTrue(registry.formulas().containsKey("FN2"));
    }

    @Test
    void pluginGetters() {
        PravaahPlugin plugin = new PravaahPlugin("test");
        assertEquals("test", plugin.getName());
        assertNull(plugin.getFormulas());
        assertNull(plugin.getValidators());
    }

    // ====================== Perf tests ======================

    @Test
    void perfHelpers() {
        ProcessStats stats = PerfUtils.createStats();
        stats.setRowsProcessed(1);
        PerfUtils.observeMemory(stats);
        ProcessStats finished = PerfUtils.finishStats(stats);
        assertTrue(finished.getDurationMs() >= 0);

        assertEquals("10B", PerfUtils.formatBytes(10));
        assertEquals("2.0KB", PerfUtils.formatBytes(2048));
        assertEquals("2.0MB", PerfUtils.formatBytes(2 * 1024 * 1024));
    }

    @Test
    void mergeStats() {
        ProcessStats a = PerfUtils.createStats();
        a.setRowsProcessed(1);
        a.setRowsWritten(2);
        a.setErrors(1);
        a.setWarnings(1);
        a.setPeakMemoryBytes(1L);

        ProcessStats b = PerfUtils.createStats();
        b.setRowsProcessed(3);
        b.setRowsWritten(4);
        b.setErrors(2);
        b.setWarnings(2);
        b.setPeakMemoryBytes(2L);

        ProcessStats merged = PerfUtils.mergeStats(a, b);
        assertEquals(4, merged.getRowsProcessed());
        assertEquals(6, merged.getRowsWritten());
        assertEquals(3, merged.getErrors());
        assertEquals(3, merged.getWarnings());
    }

    @Test
    void perfMergeNullPeakMemory() {
        ProcessStats a = PerfUtils.createStats();
        a.setPeakMemoryBytes(null);
        ProcessStats b = PerfUtils.createStats();
        b.setPeakMemoryBytes(null);
        ProcessStats merged = PerfUtils.mergeStats(a, b);
        assertEquals(0L, merged.getPeakMemoryBytes());
    }

    @Test
    void perfFormatBytesEdgeCases() {
        assertEquals("0B", PerfUtils.formatBytes(0));
        assertEquals("1023B", PerfUtils.formatBytes(1023));
        assertEquals("1.0KB", PerfUtils.formatBytes(1024));
    }

    // ====================== ProcessStats ======================

    @Test
    void processStatsIncrements() {
        ProcessStats stats = new ProcessStats();
        assertEquals(0, stats.getRowsProcessed());
        stats.incrementRowsProcessed();
        assertEquals(1, stats.getRowsProcessed());
        stats.incrementRowsWritten();
        assertEquals(1, stats.getRowsWritten());
    }

    @Test
    void processStatsAddErrorsWarnings() {
        ProcessStats stats = new ProcessStats();
        stats.addErrors(3);
        stats.addWarnings(2);
        assertEquals(3, stats.getErrors());
        assertEquals(2, stats.getWarnings());
    }

    @Test
    void processStatsSheets() {
        ProcessStats stats = new ProcessStats();
        assertTrue(stats.getSheets().isEmpty());
        stats.getSheets().add("Sheet1");
        assertEquals(1, stats.getSheets().size());
    }

    @Test
    void processStatsSetters() {
        ProcessStats stats = new ProcessStats();
        stats.setStartedAt(100L);
        assertEquals(100L, stats.getStartedAt());
        stats.setEndedAt(200L);
        assertEquals(200L, stats.getEndedAt());
        stats.setDurationMs(100L);
        assertEquals(100L, stats.getDurationMs());
    }

    // ====================== PravaahIssue ======================

    @Test
    void issueError() {
        PravaahIssue issue = PravaahIssue.error("code1", "msg1", 5, "col1", "raw", "expected");
        assertEquals("code1", issue.getCode());
        assertEquals("msg1", issue.getMessage());
        assertEquals(5, issue.getRowNumber());
        assertEquals("col1", issue.getColumn());
        assertEquals("raw", issue.getRawValue());
        assertEquals("expected", issue.getExpected());
        assertEquals(PravaahIssue.Severity.ERROR, issue.getSeverity());
    }

    @Test
    void issueWarning() {
        PravaahIssue issue = PravaahIssue.warning("code2", "msg2", 10, "col2", null, null);
        assertEquals(PravaahIssue.Severity.WARNING, issue.getSeverity());
        assertNull(issue.getRawValue());
        assertNull(issue.getExpected());
    }

    // ====================== PravaahValidationException ======================

    @Test
    void validationExceptionMessage() {
        List<PravaahIssue> issues = Collections.singletonList(
                PravaahIssue.error("test", "test message", 1, null, null, null));
        PravaahValidationException ex = new PravaahValidationException(issues);
        assertTrue(ex.getMessage().contains("1 issue"));
        assertEquals(1, ex.getIssues().size());
    }

    @Test
    void validationExceptionPlural() {
        List<PravaahIssue> issues = Arrays.asList(
                PravaahIssue.error("a", "a", 1, null, null, null),
                PravaahIssue.error("b", "b", 2, null, null, null));
        PravaahValidationException ex = new PravaahValidationException(issues);
        assertTrue(ex.getMessage().contains("2 issues"));
    }

    @Test
    void validationExceptionIssuesImmutable() {
        List<PravaahIssue> issues = new ArrayList<>();
        issues.add(PravaahIssue.error("a", "a", 1, null, null, null));
        PravaahValidationException ex = new PravaahValidationException(issues);
        assertThrows(UnsupportedOperationException.class, () -> ex.getIssues().clear());
    }

    // ====================== ProcessResult ======================

    @Test
    void processResultGetters() {
        ProcessStats stats = PerfUtils.createStats();
        List<Row> rows = Collections.singletonList(Row.of("a", 1));
        List<PravaahIssue> issues = Collections.emptyList();
        ProcessResult result = new ProcessResult(rows, issues, stats);
        assertEquals(1, result.getRows().size());
        assertTrue(result.getIssues().isEmpty());
        assertNotNull(result.getStats());
    }

    // ====================== Pipeline advanced tests ======================

    @Test
    void pipelineTake() throws Exception {
        List<Row> input = Arrays.asList(Row.of("id", 1), Row.of("id", 2), Row.of("id", 3));
        List<Row> result = Pravaah.read(input).take(2).collect();
        assertEquals(2, result.size());
    }

    @Test
    void pipelineProcess() throws Exception {
        List<Row> input = Arrays.asList(Row.of("id", 1), Row.of("id", 2));
        ProcessResult result = Pravaah.read(input).process();
        assertEquals(2, result.getRows().size());
        assertEquals(2, result.getStats().getRowsProcessed());
    }

    @Test
    void pipelineDedupeClean() throws Exception {
        List<Row> input = Arrays.asList(
                Row.of("id", "1", "email", " ada@example.com "),
                Row.of("id", "1", "email", "ada@example.com"),
                Row.of("id", "2", "email", "grace@example.com")
        );

        List<Row> result = Pravaah.read(input)
                .clean(CleaningOptions.defaults().trim(true).dedupeKey("id"))
                .collect();
        assertEquals(2, result.size());
    }

    @Test
    void pipelineFusedMapFilter() throws Exception {
        List<Row> input = Arrays.asList(
                Row.of("name", "Ada", "score", 10),
                Row.of("name", "Grace", "score", 3),
                Row.of("name", "Linus", "score", 8)
        );

        List<Row> result = Pravaah.read(input)
                .map(row -> {
                    Row r = row.copy();
                    r.put("doubled", ((Number) row.get("score")).intValue() * 2);
                    return r;
                })
                .filter(row -> ((Number) row.get("doubled")).intValue() > 10)
                .map(row -> Row.of("name", row.get("name"), "doubled", row.get("doubled")))
                .collect();

        assertEquals(2, result.size());
    }

    @Test
    void pipelineTakeMoreThanAvailable() throws Exception {
        List<Row> input = Arrays.asList(Row.of("id", 1), Row.of("id", 2));
        List<Row> result = Pravaah.read(input).take(100).collect();
        assertEquals(2, result.size());
    }

    @Test
    void pipelineSchemaWithValidation() throws Exception {
        List<Row> input = Arrays.asList(
                Row.of("id", "1"),
                Row.of("id", "bad"),
                Row.of("id", "3")
        );

        ProcessResult result = Pravaah.read(input)
                .schema(SchemaDefinition.of("id", Schema.number()), ValidationMode.COLLECT, null)
                .process();

        assertEquals(2, result.getRows().size());
        assertFalse(result.getIssues().isEmpty());
    }

    @Test
    void pipelineSchemaSkipMode() throws Exception {
        List<Row> input = Arrays.asList(
                Row.of("id", "1"),
                Row.of("id", "bad"),
                Row.of("id", "3")
        );

        ProcessResult result = Pravaah.read(input)
                .schema(SchemaDefinition.of("id", Schema.number()), ValidationMode.SKIP, null)
                .process();

        assertEquals(2, result.getRows().size());
        assertTrue(result.getIssues().isEmpty());
    }

    @Test
    void pipelineWriteJson() throws Exception {
        String file = tempDir.resolve("pipeline.json").toString();
        ProcessStats stats = Pravaah.read(Arrays.asList(Row.of("id", 1), Row.of("id", 2)))
                .write(file, WriteOptions.defaults().format(PravaahFormat.JSON));
        assertEquals(2, stats.getRowsWritten());

        List<Row> readBack = Pravaah.read(file, ReadOptions.defaults().format(PravaahFormat.JSON)).collect();
        assertEquals(2, readBack.size());
    }

    @Test
    void pipelineWriteXlsx() throws Exception {
        String file = tempDir.resolve("pipeline.xlsx").toString();
        ProcessStats stats = Pravaah.read(Arrays.asList(Row.of("name", "Ada"), Row.of("name", "Grace")))
                .write(file, WriteOptions.defaults().format(PravaahFormat.XLSX));
        assertEquals(2, stats.getRowsWritten());
    }

    @Test
    void pipelineWriteAutoDetectCsv() throws Exception {
        String file = tempDir.resolve("auto.csv").toString();
        ProcessStats stats = Pravaah.read(Arrays.asList(Row.of("id", 1)))
                .write(file, WriteOptions.defaults());
        assertEquals(1, stats.getRowsWritten());
    }

    @Test
    void pipelineWriteAutoDetectXlsx() throws Exception {
        String file = tempDir.resolve("auto.xlsx").toString();
        ProcessStats stats = Pravaah.read(Arrays.asList(Row.of("id", 1)))
                .write(file, WriteOptions.defaults());
        assertEquals(1, stats.getRowsWritten());
    }

    @Test
    void pipelineSchemaWithCleaning() throws Exception {
        List<Row> input = Collections.singletonList(Row.of("id", " 42 "));
        List<Row> result = Pravaah.read(input)
                .schema(SchemaDefinition.of("id", Schema.number()), null,
                        CleaningOptions.defaults().trim(true))
                .collect();
        assertEquals(1, result.size());
        assertEquals(42.0, result.get(0).get("id"));
    }

    // ====================== JSON tests ======================

    @Test
    void jsonRoundTrip() throws Exception {
        String file = tempDir.resolve("data.json").toString();
        Pravaah.write(Arrays.asList(Row.of("id", 1), Row.of("id", 2)), file,
                WriteOptions.defaults().format(PravaahFormat.JSON));

        List<Row> rows = Pravaah.read(file, ReadOptions.defaults().format(PravaahFormat.JSON)).collect();
        assertEquals(2, rows.size());
        assertEquals(1, rows.get(0).get("id"));
    }

    @Test
    void jsonFromBuffer() throws Exception {
        byte[] json = "[{\"id\": 3}]".getBytes(StandardCharsets.UTF_8);
        List<Row> rows = Pravaah.read(json, ReadOptions.defaults().format(PravaahFormat.JSON)).collect();
        assertEquals(1, rows.size());
        assertEquals(3, rows.get(0).get("id"));
    }

    @Test
    void jsonParseBooleans() {
        List<Row> rows = Pravaah.parseJsonRows("[{\"active\": true, \"deleted\": false}]");
        assertEquals(true, rows.get(0).get("active"));
        assertEquals(false, rows.get(0).get("deleted"));
    }

    @Test
    void jsonParseNull() {
        List<Row> rows = Pravaah.parseJsonRows("[{\"value\": null}]");
        assertNull(rows.get(0).get("value"));
    }

    @Test
    void jsonParseNestedObject() {
        List<Row> rows = Pravaah.parseJsonRows("[{\"nested\": {\"a\": 1}}]");
        assertTrue(rows.get(0).get("nested") instanceof String);
        assertTrue(((String) rows.get(0).get("nested")).contains("\"a\""));
    }

    @Test
    void jsonParseEmptyArray() {
        List<Row> rows = Pravaah.parseJsonRows("[]");
        assertTrue(rows.isEmpty());
    }

    @Test
    void jsonParseEmptyObject() {
        List<Row> rows = Pravaah.parseJsonRows("[{}]");
        assertEquals(1, rows.size());
        assertTrue(rows.get(0).isEmpty());
    }

    @Test
    void jsonParseEscapedStrings() {
        List<Row> rows = Pravaah.parseJsonRows("[{\"name\": \"O\\\"Brien\"}]");
        assertEquals("O\"Brien", rows.get(0).get("name"));
    }

    @Test
    void jsonParseFloat() {
        List<Row> rows = Pravaah.parseJsonRows("[{\"v\": 3.14}]");
        assertEquals(3.14, ((Number) rows.get(0).get("v")).doubleValue(), 0.001);
    }

    @Test
    void jsonParseNotArray() {
        assertThrows(IllegalArgumentException.class, () -> Pravaah.parseJsonRows("{\"a\": 1}"));
    }

    @Test
    void jsonWriteWithNulls() throws Exception {
        String file = tempDir.resolve("nulls.json").toString();
        Row row = Row.of("a", 1);
        row.put("b", null);
        row.put("c", true);
        row.put("d", "text");
        Pravaah.write(Collections.singletonList(row), file,
                WriteOptions.defaults().format(PravaahFormat.JSON));
        String content = new String(Files.readAllBytes(Paths.get(file)));
        assertTrue(content.contains("null"));
        assertTrue(content.contains("true"));
        assertTrue(content.contains("\"text\""));
    }

    @Test
    void jsonWriteSpecialChars() throws Exception {
        String file = tempDir.resolve("special.json").toString();
        Pravaah.write(Collections.singletonList(Row.of("name", "O\"Brien")), file,
                WriteOptions.defaults().format(PravaahFormat.JSON));
        String content = new String(Files.readAllBytes(Paths.get(file)));
        assertTrue(content.contains("O\\\"Brien"));
    }

    // ====================== Write pipeline ======================

    @Test
    void pipelineWrite() throws Exception {
        String file = tempDir.resolve("pipeline.csv").toString();
        ProcessStats stats = Pravaah.read(Arrays.asList(Row.of("id", 1), Row.of("id", 2)))
                .write(file, WriteOptions.defaults().format(PravaahFormat.CSV));
        assertEquals(2, stats.getRowsWritten());
    }

    // ====================== Pravaah static methods ======================

    @Test
    void pravaahReadFromFileAutoFormat() throws Exception {
        String file = tempDir.resolve("auto.csv").toString();
        Pravaah.write(Collections.singletonList(Row.of("x", 1)), file,
                WriteOptions.defaults().format(PravaahFormat.CSV));
        List<Row> rows = Pravaah.read(file).collect();
        assertEquals(1, rows.size());
    }

    @Test
    void pravaahWriteAutoFormat() throws Exception {
        String file = tempDir.resolve("auto.json").toString();
        Pravaah.write(Collections.singletonList(Row.of("x", 1)), file);
        List<Row> rows = Pravaah.read(file, ReadOptions.defaults().format(PravaahFormat.JSON)).collect();
        assertEquals(1, rows.size());
    }

    @Test
    void pravaahReadFromByteArrayDefaultFormat() throws Exception {
        String file = tempDir.resolve("bytes.xlsx").toString();
        Pravaah.write(Collections.singletonList(Row.of("x", 1)), file,
                WriteOptions.defaults().format(PravaahFormat.XLSX));
        byte[] data = Files.readAllBytes(Paths.get(file));
        List<Row> rows = Pravaah.read(data, ReadOptions.defaults()).collect();
        assertEquals(1, rows.size());
    }

    @Test
    void pravaahParseBytesWithSchema() throws Exception {
        byte[] csv = "id,name\n1,Ada\n2,Grace\n".getBytes(StandardCharsets.UTF_8);
        List<Row> rows = Pravaah.parse(csv,
                SchemaDefinition.of("id", Schema.number(), "name", Schema.string()),
                ReadOptions.defaults().format(PravaahFormat.CSV));
        assertEquals(2, rows.size());
        assertEquals(1.0, rows.get(0).get("id"));
    }

    @Test
    void pravaahParseFileWithSchema() throws Exception {
        String file = tempDir.resolve("parse.csv").toString();
        Pravaah.write(Arrays.asList(Row.of("id", 1), Row.of("id", 2)), file,
                WriteOptions.defaults().format(PravaahFormat.CSV));

        List<Row> rows = Pravaah.parse(file,
                SchemaDefinition.of("id", Schema.number()),
                ReadOptions.defaults().format(PravaahFormat.CSV));
        assertEquals(2, rows.size());
    }

    @Test
    void pravaahParseDetailedFromFile() throws Exception {
        String file = tempDir.resolve("detailed.csv").toString();
        Files.write(Paths.get(file), "email\nbad\nada@example.com\n".getBytes(StandardCharsets.UTF_8));

        ProcessResult result = Pravaah.parseDetailed(file,
                SchemaDefinition.of("email", Schema.email()),
                ReadOptions.defaults().format(PravaahFormat.CSV).validation(ValidationMode.COLLECT));
        assertEquals(1, result.getRows().size());
        assertEquals(1, result.getIssues().size());
    }

    @Test
    void pravaahParseDetailedWithDeduplication() throws Exception {
        byte[] csv = "id,name\n1,Ada\n1,Ada\n2,Grace\n".getBytes(StandardCharsets.UTF_8);
        ProcessResult result = Pravaah.parseDetailed(csv,
                SchemaDefinition.of("id", Schema.number(), "name", Schema.string()),
                ReadOptions.defaults().format(PravaahFormat.CSV).validation(ValidationMode.COLLECT)
                        .cleaning(CleaningOptions.defaults().dedupeKey("id")));
        assertEquals(2, result.getRows().size());
    }

    @Test
    void pravaahParseDetailedFailFast() {
        byte[] csv = "id\nbad\n1\n".getBytes(StandardCharsets.UTF_8);
        assertThrows(PravaahValidationException.class, () ->
                Pravaah.parseDetailed(csv,
                        SchemaDefinition.of("id", Schema.number()),
                        ReadOptions.defaults().format(PravaahFormat.CSV).validation(ValidationMode.FAIL_FAST)));
    }

    @Test
    void pravaahParseDetailedSkipMode() throws Exception {
        byte[] csv = "id\nbad\n1\n".getBytes(StandardCharsets.UTF_8);
        ProcessResult result = Pravaah.parseDetailed(csv,
                SchemaDefinition.of("id", Schema.number()),
                ReadOptions.defaults().format(PravaahFormat.CSV).validation(ValidationMode.SKIP));
        assertEquals(1, result.getRows().size());
        assertTrue(result.getIssues().isEmpty());
    }

    @Test
    void pravaahEvaluateFormula() {
        assertEquals(6.0, Pravaah.evaluateFormula("=SUM(a,b)", Row.of("a", 2, "b", 4)));
    }

    @Test
    void pravaahQuery() {
        List<Row> rows = Collections.singletonList(Row.of("id", 1, "name", "Ada"));
        List<Row> result = Pravaah.query(rows, "SELECT name");
        assertEquals("Ada", result.get(0).get("name"));
    }

    @Test
    void pravaahCreateIndex() {
        List<Row> rows = Arrays.asList(Row.of("k", "a"), Row.of("k", "a"), Row.of("k", "b"));
        Map<String, List<Row>> index = Pravaah.createIndex(rows, "k");
        assertEquals(2, index.get("a").size());
    }

    @Test
    void pravaahJoinRows() {
        List<Row> left = Collections.singletonList(Row.of("id", 1, "x", "a"));
        List<Row> right = Collections.singletonList(Row.of("id", 1, "y", "b"));
        List<Row> joined = Pravaah.joinRows(left, right, "id");
        assertEquals(1, joined.size());
        assertEquals("a", joined.get(0).get("x"));
        assertEquals("b", joined.get(0).get("y"));
    }

    // ====================== CSV value inference ======================

    @Test
    void csvValueInference() {
        assertNull(CsvReader.inferValue(""));
        assertEquals(42, CsvReader.inferValue("42"));
        assertEquals(false, CsvReader.inferValue("FALSE"));
        assertEquals("Ada", CsvReader.inferValue("Ada"));
    }

    // ====================== ReadOptions / WriteOptions / CleaningOptions ======================

    @Test
    void readOptionsChaining() {
        ReadOptions opts = ReadOptions.defaults()
                .format(PravaahFormat.CSV)
                .sheetName("Sheet1")
                .sheetIndex(0)
                .headers(true)
                .headerNames(Arrays.asList("a", "b"))
                .delimiter(";")
                .inferTypes(true)
                .formulas("preserve")
                .validation(ValidationMode.COLLECT)
                .cleaning(CleaningOptions.defaults());

        assertEquals(PravaahFormat.CSV, opts.getFormat());
        assertEquals("Sheet1", opts.getSheetName());
        assertEquals(0, opts.getSheetIndex().intValue());
        assertTrue(opts.getHeaders());
        assertEquals(2, opts.getHeaderNames().size());
        assertEquals(";", opts.getDelimiter());
        assertTrue(opts.isInferTypes());
        assertEquals("preserve", opts.getFormulas());
        assertEquals(ValidationMode.COLLECT, opts.getValidation());
        assertNotNull(opts.getCleaning());
    }

    @Test
    void writeOptionsChaining() {
        WriteOptions opts = WriteOptions.defaults()
                .format(PravaahFormat.XLSX)
                .sheetName("Data")
                .headers(Arrays.asList("col1", "col2"))
                .delimiter("\t");

        assertEquals(PravaahFormat.XLSX, opts.getFormat());
        assertEquals("Data", opts.getSheetName());
        assertEquals(2, opts.getHeaders().size());
        assertEquals("\t", opts.getDelimiter());
    }

    @Test
    void cleaningOptionsChaining() {
        CleaningOptions opts = CleaningOptions.defaults()
                .trim(true)
                .normalizeWhitespace(true)
                .dedupeKey("id", "name")
                .fuzzyHeader("email", "e-mail", "Email Address");

        assertTrue(opts.isTrim());
        assertTrue(opts.isNormalizeWhitespace());
        assertEquals(2, opts.getDedupeKey().size());
        assertEquals(1, opts.getFuzzyHeaders().size());
        assertEquals(2, opts.getFuzzyHeaders().get("email").size());
    }

    @Test
    void cleaningOptionsFuzzyHeadersMap() {
        Map<String, List<String>> map = new LinkedHashMap<>();
        map.put("email", Arrays.asList("e-mail", "Email Address"));
        CleaningOptions opts = CleaningOptions.defaults().fuzzyHeaders(map);
        assertEquals(1, opts.getFuzzyHeaders().size());
    }

    // ====================== Schema builders ======================

    @Test
    void schemaBuilders() {
        FieldDefinition str = Schema.string();
        assertEquals(FieldKind.STRING, str.getKind());
        assertTrue(str.isCoerce());
        assertFalse(str.isOptional());

        FieldDefinition num = Schema.number(true);
        assertEquals(FieldKind.NUMBER, num.getKind());
        assertTrue(num.isOptional());

        FieldDefinition bool = Schema.bool(true);
        assertEquals(FieldKind.BOOLEAN, bool.getKind());

        FieldDefinition date = Schema.date(true);
        assertEquals(FieldKind.DATE, date.getKind());

        FieldDefinition email = Schema.email(true);
        assertEquals(FieldKind.EMAIL, email.getKind());

        FieldDefinition phone = Schema.phone(true);
        assertEquals(FieldKind.PHONE, phone.getKind());

        FieldDefinition any = Schema.any();
        assertEquals(FieldKind.ANY, any.getKind());
    }

    @Test
    void fieldDefinitionDefaultValue() {
        FieldDefinition fd = Schema.string().defaultValue("hello");
        assertTrue(fd.hasDefaultValue());
        assertEquals("hello", fd.getDefaultValue());
    }

    @Test
    void fieldDefinitionNoDefault() {
        FieldDefinition fd = Schema.string();
        assertFalse(fd.hasDefaultValue());
    }

    @Test
    void schemaDefinitionFactory() {
        SchemaDefinition sd1 = SchemaDefinition.of("a", Schema.string());
        assertEquals(1, sd1.size());

        SchemaDefinition sd2 = SchemaDefinition.of("a", Schema.string(), "b", Schema.number());
        assertEquals(2, sd2.size());

        SchemaDefinition sd3 = SchemaDefinition.of("a", Schema.string(), "b", Schema.number(), "c", Schema.bool());
        assertEquals(3, sd3.size());
    }

    @Test
    void schemaDefinitionField() {
        SchemaDefinition sd = new SchemaDefinition()
                .field("a", Schema.string())
                .field("b", Schema.number());
        assertEquals(2, sd.size());
    }

    // ====================== ValidationMode enum ======================

    @Test
    void validationModeValues() {
        ValidationMode[] modes = ValidationMode.values();
        assertTrue(modes.length >= 3);
        assertNotNull(ValidationMode.valueOf("COLLECT"));
        assertNotNull(ValidationMode.valueOf("FAIL_FAST"));
        assertNotNull(ValidationMode.valueOf("SKIP"));
    }

    // ====================== FieldKind enum ======================

    @Test
    void fieldKindValues() {
        FieldKind[] kinds = FieldKind.values();
        assertTrue(kinds.length >= 7);
        assertNotNull(FieldKind.valueOf("STRING"));
        assertNotNull(FieldKind.valueOf("NUMBER"));
        assertNotNull(FieldKind.valueOf("BOOLEAN"));
        assertNotNull(FieldKind.valueOf("DATE"));
        assertNotNull(FieldKind.valueOf("EMAIL"));
        assertNotNull(FieldKind.valueOf("PHONE"));
        assertNotNull(FieldKind.valueOf("ANY"));
    }

    // ====================== Issue report with warning ======================

    @Test
    void issueReportWithWarnings() throws Exception {
        List<PravaahIssue> issues = Arrays.asList(
                PravaahIssue.error("err", "error msg", 1, "col1", "raw1", "expected1"),
                PravaahIssue.warning("warn", "warning msg", 2, null, null, null)
        );
        String file = tempDir.resolve("mixed_issues.csv").toString();
        SchemaValidator.writeIssueReport(issues, file);
        String content = new String(Files.readAllBytes(Paths.get(file)));
        assertTrue(content.contains("error"));
        assertTrue(content.contains("warning"));
    }

    // ====================== Normalization ======================

    @Test
    void normalizeHeaderVariants() {
        assertEquals("email address", SchemaValidator.normalizeHeader("  Email_Address  "));
        assertEquals("first name", SchemaValidator.normalizeHeader("First-Name"));
        assertEquals("id", SchemaValidator.normalizeHeader("  ID  "));
    }

    private static void assertRowsExactly(List<Row> actual, Row... expected) {
        assertEquals(Arrays.asList(expected), actual);
    }

    private static byte[] sampleXlsWorkbook() throws IOException {
        TestSheet data = new TestSheet("Data", new Object[][] {
                {"email", "score", "active", "note", "blank", "computed"},
                {"ada@example.com", 42, true, "hello, world", null, formula(84)},
                {"grace@example.com", 7.5, false, "escaped \"quote\"", null, formula(15)}
        });
        TestSheet second = new TestSheet("Second", new Object[][] {
                {"kind", "value"},
                {"secondary", 99}
        });
        return createXlsWorkbook(data, second);
    }

    private static FormulaValue formula(double value) {
        return new FormulaValue(value);
    }

    private static byte[] createXlsWorkbook(TestSheet... sheets) throws IOException {
        List<String> sharedStrings = new ArrayList<>();
        Map<String, Integer> sharedStringIndexes = new LinkedHashMap<>();
        for (TestSheet sheet : sheets) {
            for (Object[] row : sheet.rows) {
                for (Object value : row) {
                    if (value instanceof String && !sharedStringIndexes.containsKey(value)) {
                        sharedStringIndexes.put((String) value, sharedStrings.size());
                        sharedStrings.add((String) value);
                    }
                }
            }
        }

        List<byte[]> sheetStreams = new ArrayList<>();
        for (TestSheet sheet : sheets) {
            sheetStreams.add(createSheetStream(sheet, sharedStringIndexes));
        }

        byte[] globalsBof = record(0x0809, bofPayload(0x0005));
        byte[] sst = createSstRecord(sharedStrings);
        byte[] globalsEof = record(0x000A, new byte[0]);

        int boundsLength = 0;
        for (TestSheet sheet : sheets) boundsLength += 4 + boundsheetPayload(0, sheet.name).length;
        int offset = globalsBof.length + sst.length + boundsLength + globalsEof.length;

        ByteArrayOutputStream workbook = new ByteArrayOutputStream();
        workbook.write(globalsBof);
        workbook.write(sst);
        for (int i = 0; i < sheets.length; i++) {
            workbook.write(record(0x0085, boundsheetPayload(offset, sheets[i].name)));
            offset += sheetStreams.get(i).length;
        }
        workbook.write(globalsEof);
        for (byte[] sheetStream : sheetStreams) workbook.write(sheetStream);

        while (workbook.size() < 4096) {
            workbook.write(0);
        }
        return createOleWorkbook(workbook.toByteArray());
    }

    private static byte[] createSheetStream(TestSheet sheet, Map<String, Integer> sharedStringIndexes) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(record(0x0809, bofPayload(0x0010)));
        out.write(record(0x0200, dimensionsPayload(sheet.rows.length, maxColumns(sheet.rows))));
        for (int r = 0; r < sheet.rows.length; r++) {
            Object[] row = sheet.rows[r];
            for (int c = 0; c < row.length; c++) {
                Object value = row[c];
                if (value == null) {
                    out.write(record(0x0201, rowColXfPayload(r, c)));
                } else if (value instanceof String) {
                    out.write(record(0x00FD, labelSstPayload(r, c, sharedStringIndexes.get(value))));
                } else if (value instanceof Boolean) {
                    out.write(record(0x0205, boolPayload(r, c, (Boolean) value)));
                } else if (value instanceof FormulaValue) {
                    out.write(record(0x0006, formulaPayload(r, c, ((FormulaValue) value).value)));
                } else if (value instanceof Number) {
                    out.write(record(0x0203, numberPayload(r, c, ((Number) value).doubleValue())));
                }
            }
        }
        out.write(record(0x000A, new byte[0]));
        return out.toByteArray();
    }

    private static byte[] createSstRecord(List<String> strings) throws IOException {
        ByteArrayOutputStream payload = new ByteArrayOutputStream();
        writeInt(payload, strings.size());
        writeInt(payload, strings.size());
        for (String value : strings) writeUnicodeString(payload, value);
        return record(0x00FC, payload.toByteArray());
    }

    private static byte[] createOleWorkbook(byte[] workbookStream) throws IOException {
        final int sectorSize = 512;
        int workbookSectors = (workbookStream.length + sectorSize - 1) / sectorSize;
        int sectorCount = 2 + workbookSectors;
        int[] fat = new int[Math.max(128, sectorCount)];
        Arrays.fill(fat, -1);
        fat[0] = -3;
        fat[1] = -2;
        for (int i = 0; i < workbookSectors; i++) {
            fat[2 + i] = i == workbookSectors - 1 ? -2 : 3 + i;
        }

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] header = new byte[512];
        byte[] signature = new byte[] {
                (byte) 0xD0, (byte) 0xCF, 0x11, (byte) 0xE0,
                (byte) 0xA1, (byte) 0xB1, 0x1A, (byte) 0xE1
        };
        System.arraycopy(signature, 0, header, 0, signature.length);
        writeShort(header, 24, 0x003E);
        writeShort(header, 26, 0x0003);
        writeShort(header, 28, 0xFFFE);
        writeShort(header, 30, 9);
        writeShort(header, 32, 6);
        writeInt(header, 44, 1);
        writeInt(header, 48, 1);
        writeInt(header, 56, 4096);
        writeInt(header, 60, -2);
        writeInt(header, 64, 0);
        writeInt(header, 68, -2);
        writeInt(header, 72, 0);
        for (int i = 0; i < 109; i++) writeInt(header, 76 + i * 4, -1);
        writeInt(header, 76, 0);
        out.write(header);

        byte[] fatSector = new byte[512];
        for (int i = 0; i < 128; i++) writeInt(fatSector, i * 4, fat[i]);
        out.write(fatSector);
        out.write(directorySector(workbookStream.length));

        byte[] paddedWorkbook = new byte[workbookSectors * sectorSize];
        System.arraycopy(workbookStream, 0, paddedWorkbook, 0, workbookStream.length);
        out.write(paddedWorkbook);
        return out.toByteArray();
    }

    private static byte[] directorySector(int workbookSize) {
        byte[] directory = new byte[512];
        directoryEntry(directory, 0, "Root Entry", 5, -1, 0);
        directoryEntry(directory, 128, "Workbook", 2, 2, workbookSize);
        return directory;
    }

    private static void directoryEntry(byte[] directory, int offset, String name, int type, int startSector, int size) {
        for (int i = 0; i < name.length(); i++) {
            writeShort(directory, offset + i * 2, name.charAt(i));
        }
        writeShort(directory, offset + name.length() * 2, 0);
        writeShort(directory, offset + 64, (name.length() + 1) * 2);
        directory[offset + 66] = (byte) type;
        directory[offset + 67] = 1;
        writeInt(directory, offset + 68, -1);
        writeInt(directory, offset + 72, -1);
        writeInt(directory, offset + 76, type == 5 ? 1 : -1);
        writeInt(directory, offset + 116, startSector);
        writeInt(directory, offset + 120, size);
        writeInt(directory, offset + 124, 0);
    }

    private static byte[] bofPayload(int type) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writeShort(out, 0x0600);
        writeShort(out, type);
        writeShort(out, 0x0DBB);
        writeShort(out, 0x07CC);
        writeInt(out, 0x00000041);
        writeInt(out, 0x00000006);
        return out.toByteArray();
    }

    private static byte[] boundsheetPayload(int offset, String name) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writeInt(out, offset);
        out.write(0);
        out.write(0);
        writeByteCountUnicodeString(out, name);
        return out.toByteArray();
    }

    private static byte[] dimensionsPayload(int rows, int cols) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writeInt(out, 0);
        writeInt(out, rows);
        writeShort(out, 0);
        writeShort(out, cols);
        writeShort(out, 0);
        return out.toByteArray();
    }

    private static byte[] rowColXfPayload(int row, int col) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writeShort(out, row);
        writeShort(out, col);
        writeShort(out, 0);
        return out.toByteArray();
    }

    private static byte[] labelSstPayload(int row, int col, int sstIndex) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(rowColXfPayload(row, col));
        writeInt(out, sstIndex);
        return out.toByteArray();
    }

    private static byte[] boolPayload(int row, int col, boolean value) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(rowColXfPayload(row, col));
        out.write(value ? 1 : 0);
        out.write(0);
        return out.toByteArray();
    }

    private static byte[] numberPayload(int row, int col, double value) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(rowColXfPayload(row, col));
        writeLong(out, Double.doubleToLongBits(value));
        return out.toByteArray();
    }

    private static byte[] formulaPayload(int row, int col, double cachedValue) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(numberPayload(row, col, cachedValue));
        writeShort(out, 0);
        writeInt(out, 0);
        writeShort(out, 0);
        return out.toByteArray();
    }

    private static byte[] record(int sid, byte[] payload) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writeShort(out, sid);
        writeShort(out, payload.length);
        out.write(payload);
        return out.toByteArray();
    }

    private static void writeUnicodeString(ByteArrayOutputStream out, String value) throws IOException {
        writeShort(out, value.length());
        out.write(0);
        for (int i = 0; i < value.length(); i++) out.write((byte) value.charAt(i));
    }

    private static void writeByteCountUnicodeString(ByteArrayOutputStream out, String value) throws IOException {
        out.write(value.length());
        out.write(0);
        for (int i = 0; i < value.length(); i++) out.write((byte) value.charAt(i));
    }

    private static int maxColumns(Object[][] rows) {
        int max = 0;
        for (Object[] row : rows) max = Math.max(max, row.length);
        return max;
    }

    private static void writeShort(ByteArrayOutputStream out, int value) throws IOException {
        out.write(value & 0xFF);
        out.write((value >>> 8) & 0xFF);
    }

    private static void writeInt(ByteArrayOutputStream out, int value) throws IOException {
        out.write(value & 0xFF);
        out.write((value >>> 8) & 0xFF);
        out.write((value >>> 16) & 0xFF);
        out.write((value >>> 24) & 0xFF);
    }

    private static void writeLong(ByteArrayOutputStream out, long value) throws IOException {
        writeInt(out, (int) value);
        writeInt(out, (int) (value >>> 32));
    }

    private static void writeShort(byte[] bytes, int offset, int value) {
        bytes[offset] = (byte) value;
        bytes[offset + 1] = (byte) (value >>> 8);
    }

    private static void writeInt(byte[] bytes, int offset, int value) {
        bytes[offset] = (byte) value;
        bytes[offset + 1] = (byte) (value >>> 8);
        bytes[offset + 2] = (byte) (value >>> 16);
        bytes[offset + 3] = (byte) (value >>> 24);
    }

    private static final class TestSheet {
        final String name;
        final Object[][] rows;

        TestSheet(String name, Object[][] rows) {
            this.name = name;
            this.rows = rows;
        }
    }

    private static final class FormulaValue {
        final double value;

        FormulaValue(double value) {
            this.value = value;
        }
    }
}
