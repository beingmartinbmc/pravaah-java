package io.github.beingmartinbmc.pravaah;

import io.github.beingmartinbmc.pravaah.csv.CsvReader;
import io.github.beingmartinbmc.pravaah.diff.DiffEngine;
import io.github.beingmartinbmc.pravaah.formula.FormulaEngine;
import io.github.beingmartinbmc.pravaah.perf.PerfUtils;
import io.github.beingmartinbmc.pravaah.pipeline.PravaahPipeline;
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
import java.util.*;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;

class PravaahTest {

    @TempDir
    Path tempDir;

    // --- Pipeline tests ---

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

    // --- CSV tests ---

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
        assertEquals(2, rows.size());
        assertEquals("", rows.get(0).get("b"));
    }

    @Test
    void csvNoTrailingNewline() throws Exception {
        byte[] csv = "x,y\n1,2".getBytes(StandardCharsets.UTF_8);
        List<Row> rows = CsvReader.readAll(csv, ReadOptions.defaults().format(PravaahFormat.CSV));
        assertEquals(1, rows.size());
        assertEquals("1", rows.get(0).get("x"));
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
        assertEquals(2, rows.size());
        assertEquals("1", rows.get(0).get("id"));
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

    // --- XLSX tests ---

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

    // --- Schema tests ---

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

    // --- Formula tests ---

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

    // --- Query tests ---

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

    // --- Diff tests ---

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

    // --- Plugin tests ---

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

    // --- Perf tests ---

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

    // --- Pipeline advanced tests ---

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

    // --- JSON tests ---

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

    // --- Write pipeline ---

    @Test
    void pipelineWrite() throws Exception {
        String file = tempDir.resolve("pipeline.csv").toString();
        ProcessStats stats = Pravaah.read(Arrays.asList(Row.of("id", 1), Row.of("id", 2)))
                .write(file, WriteOptions.defaults().format(PravaahFormat.CSV));
        assertEquals(2, stats.getRowsWritten());
    }

    // --- CSV value inference ---

    @Test
    void csvValueInference() {
        assertNull(CsvReader.inferValue(""));
        assertEquals(42, CsvReader.inferValue("42"));
        assertEquals(false, CsvReader.inferValue("FALSE"));
        assertEquals("Ada", CsvReader.inferValue("Ada"));
    }
}
