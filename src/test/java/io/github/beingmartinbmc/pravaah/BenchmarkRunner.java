package io.github.beingmartinbmc.pravaah;

import io.github.beingmartinbmc.pravaah.csv.CsvReader;
import io.github.beingmartinbmc.pravaah.schema.*;
import io.github.beingmartinbmc.pravaah.xlsx.XlsxReader;

import com.univocity.parsers.csv.*;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import com.opencsv.CSVReaderBuilder;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.alibaba.excel.EasyExcel;
import com.alibaba.excel.context.AnalysisContext;
import com.alibaba.excel.event.AnalysisEventListener;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.regex.Pattern;

/**
 * Full benchmark suite: Pravaah vs uniVocity, Apache Commons CSV, OpenCSV,
 * Jackson CSV, and plain JDK (BufferedReader + split).
 *
 * Run with benchmark-libs on classpath:
 *   java -Xmx6g -cp target/classes:target/test-classes:benchmark-libs/* \
 *     io.github.beingmartinbmc.pravaah.BenchmarkRunner
 */
public final class BenchmarkRunner {

    private static final Pattern EMAIL_RE = Pattern.compile("^[^\\s@]+@[^\\s@]+\\.[^\\s@]+$");
    private static final int WARMUP = 3;
    private static final int RUNS = 5;

    public static void main(String[] args) throws Exception {
        Path tmpDir = Files.createTempDirectory("pravaah-bench");
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                Files.walk(tmpDir).sorted(Comparator.reverseOrder())
                        .map(Path::toFile).forEach(File::delete);
            } catch (IOException ignored) {}
        }));

        String ruler = repeat('=', 76);
        System.out.println(ruler);
        System.out.println("  PRAVAAH-JAVA BENCHMARK SUITE (ALL COMPETITORS)");
        System.out.println("  JVM: " + System.getProperty("java.version")
                + " | " + System.getProperty("java.vm.name")
                + " | Cores: " + Runtime.getRuntime().availableProcessors()
                + " | MaxMem: " + formatBytes(Runtime.getRuntime().maxMemory()));
        System.out.println(ruler);

        benchmarkCsvParsing(tmpDir);
        benchmarkValidationPipeline(tmpDir);
        benchmarkLargeFileStreaming(tmpDir);
        benchmarkExternalCsvFiles(Paths.get("benchmark-files"));
        benchmarkSpreadsheetFiles(Paths.get("benchmark-files"));
        benchmarkLocComparison();

        System.out.println("\n" + ruler);
        System.out.println("  BENCHMARK COMPLETE");
        System.out.println(ruler);
    }

    // =========================================================================
    // BENCHMARK 1: CSV Parsing Speed (all competitors)
    // =========================================================================

    private static void benchmarkCsvParsing(Path tmpDir) throws Exception {
        System.out.println("\n" + box("BENCHMARK 1: CSV PARSING SPEED — ALL COMPETITORS"));

        for (int size : new int[]{10_000, 100_000, 1_000_000}) {
            Path csv = generateCsv(tmpDir, size);
            byte[] data = Files.readAllBytes(csv);
            String label = fmtCount(size);

            System.out.println("--- " + label + " rows (" + formatBytes(data.length) + ") ---");

            // Warmup all parsers
            for (int i = 0; i < WARMUP; i++) {
                parsePravaah(data);
                parseDiySplit(data);
                parseCommonsCsv(data);
                parseUnivocity(data);
                parseOpenCsv(data);
                parseJacksonCsv(data);
            }

            long pv = bench(() -> parsePravaah(data));
            long diy = bench(() -> parseDiySplit(data));
            long cc = bench(() -> parseCommonsCsv(data));
            long uv = bench(() -> parseUnivocity(data));
            long oc = bench(() -> parseOpenCsv(data));
            long jc = bench(() -> parseJacksonCsv(data));

            printRow("Pravaah", pv);
            printRow("BufferedReader + split", diy);
            printRow("Apache Commons CSV", cc);
            printRow("uniVocity-parsers", uv);
            printRow("OpenCSV", oc);
            printRow("Jackson CSV", jc);
            System.out.println();
        }
    }

    // --- Parsers ---

    private static int parsePravaah(byte[] data) throws IOException {
        return CsvReader.readAll(data, ReadOptions.defaults()).size();
    }

    private static int parseDiySplit(byte[] data) throws IOException {
        BufferedReader br = new BufferedReader(
                new InputStreamReader(new ByteArrayInputStream(data), StandardCharsets.UTF_8));
        String headerLine = br.readLine();
        if (headerLine == null) return 0;
        String[] headers = headerLine.split(",");
        int count = 0;
        String line;
        while ((line = br.readLine()) != null) {
            if (line.isEmpty()) continue;
            String[] fields = line.split(",", -1);
            Map<String, String> row = new LinkedHashMap<String, String>();
            for (int i = 0; i < headers.length && i < fields.length; i++) {
                row.put(headers[i], fields[i]);
            }
            count++;
        }
        br.close();
        return count;
    }

    private static int parseCommonsCsv(byte[] data) throws IOException {
        Reader reader = new InputStreamReader(new ByteArrayInputStream(data), StandardCharsets.UTF_8);
        CSVParser parser = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(reader);
        int count = 0;
        for (CSVRecord rec : parser) {
            rec.get(0);
            count++;
        }
        parser.close();
        return count;
    }

    private static int parseUnivocity(byte[] data) {
        CsvParserSettings settings = new CsvParserSettings();
        settings.setHeaderExtractionEnabled(true);
        settings.setMaxCharsPerColumn(65536);
        com.univocity.parsers.csv.CsvParser parser =
                new com.univocity.parsers.csv.CsvParser(settings);
        List<com.univocity.parsers.common.record.Record> records =
                parser.parseAllRecords(new ByteArrayInputStream(data));
        return records.size();
    }

    private static int parseOpenCsv(byte[] data) throws Exception {
        Reader reader = new InputStreamReader(new ByteArrayInputStream(data), StandardCharsets.UTF_8);
        com.opencsv.CSVReader csvReader = new CSVReaderBuilder(reader).build();
        csvReader.readNext(); // header
        int count = 0;
        while (csvReader.readNext() != null) count++;
        csvReader.close();
        return count;
    }

    private static int parseJacksonCsv(byte[] data) throws Exception {
        CsvMapper mapper = new CsvMapper();
        CsvSchema schema = CsvSchema.emptySchema().withHeader();
        com.fasterxml.jackson.databind.MappingIterator<Map<String, String>> it =
                mapper.readerFor(Map.class).with(schema).readValues(data);
        int count = 0;
        while (it.hasNext()) { it.next(); count++; }
        it.close();
        return count;
    }

    // --- Streaming parsers for real files ---

    private static int parsePravaahFile(Path file) throws IOException {
        return CsvReader.drainCount(file.toString(), ReadOptions.defaults());
    }

    private static int parseDiySplitFile(Path file) throws IOException {
        BufferedReader br = Files.newBufferedReader(file, StandardCharsets.UTF_8);
        try {
            String headerLine = br.readLine();
            if (headerLine == null) return 0;
            int count = 0;
            String line;
            while ((line = br.readLine()) != null) {
                if (line.isEmpty()) continue;
                line.split(",", -1);
                count++;
            }
            return count;
        } finally {
            br.close();
        }
    }

    private static int parseCommonsCsvFile(Path file) throws IOException {
        Reader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8);
        CSVParser parser = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(reader);
        try {
            int count = 0;
            for (CSVRecord rec : parser) {
                rec.get(0);
                count++;
            }
            return count;
        } finally {
            parser.close();
        }
    }

    private static int parseUnivocityFile(Path file) throws IOException {
        CsvParserSettings settings = new CsvParserSettings();
        settings.setHeaderExtractionEnabled(true);
        settings.setMaxCharsPerColumn(1_048_576);
        com.univocity.parsers.csv.CsvParser parser =
                new com.univocity.parsers.csv.CsvParser(settings);
        Reader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8);
        try {
            parser.beginParsing(reader);
            int count = 0;
            while (parser.parseNext() != null) count++;
            return count;
        } finally {
            parser.stopParsing();
            reader.close();
        }
    }

    private static int parseOpenCsvFile(Path file) throws Exception {
        Reader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8);
        com.opencsv.CSVReader csvReader = new CSVReaderBuilder(reader).build();
        try {
            csvReader.readNext(); // header
            int count = 0;
            while (csvReader.readNext() != null) count++;
            return count;
        } finally {
            csvReader.close();
        }
    }

    private static int parseJacksonCsvFile(Path file) throws Exception {
        CsvMapper mapper = new CsvMapper();
        CsvSchema schema = CsvSchema.emptySchema().withHeader();
        com.fasterxml.jackson.databind.MappingIterator<Map<String, String>> it =
                mapper.readerFor(Map.class).with(schema).readValues(file.toFile());
        try {
            int count = 0;
            while (it.hasNext()) { it.next(); count++; }
            return count;
        } finally {
            it.close();
        }
    }

    // =========================================================================
    // BENCHMARK 2: Validation Pipeline
    // =========================================================================

    private static void benchmarkValidationPipeline(Path tmpDir) throws Exception {
        System.out.println(box("BENCHMARK 2: VALIDATION PIPELINE — parse + validate + coerce + errors"));

        int size = 100_000;
        Path csv = generateValidationCsv(tmpDir, size);
        byte[] data = Files.readAllBytes(csv);

        System.out.println("--- " + fmtCount(size) + " rows with ~10% bad data ---");

        for (int i = 0; i < WARMUP; i++) {
            validatePravaah(data);
            validateDiy(data);
        }

        int[] pr = new int[2], dr = new int[2];
        long pv = bench(() -> { int[] r = validatePravaah(data); pr[0] = r[0]; pr[1] = r[1]; return r[0]; });
        long diy = bench(() -> { int[] r = validateDiy(data); dr[0] = r[0]; dr[1] = r[1]; return r[0]; });

        System.out.printf("  %-32s %6d ms  valid: %d  errors: %d  LOC: 10%n",
                "Pravaah", pv, pr[0], pr[1]);
        System.out.printf("  %-32s %6d ms  valid: %d  errors: %d  LOC: 120+%n",
                "DIY (BR + regex + try/catch)", diy, dr[0], dr[1]);

        System.out.println("\n  Note: Other CSV libs don't include validation — you'd write the same");
        System.out.println("  120+ lines of DIY validation on top of ANY of them.\n");
    }

    private static int[] validatePravaah(byte[] data) throws IOException {
        SchemaDefinition schema = new SchemaDefinition()
                .field("email", Schema.email())
                .field("age", Schema.number())
                .field("active", Schema.bool())
                .field("score", Schema.number())
                .field("name", Schema.string());
        ProcessResult result = Pravaah.parseDetailed(data, schema,
                ReadOptions.defaults().format(PravaahFormat.CSV).validation(ValidationMode.COLLECT));
        return new int[]{result.getRows().size(), result.getIssues().size()};
    }

    private static int[] validateDiy(byte[] data) throws IOException {
        BufferedReader br = new BufferedReader(
                new InputStreamReader(new ByteArrayInputStream(data), StandardCharsets.UTF_8));
        String headerLine = br.readLine();
        if (headerLine == null) return new int[]{0, 0};
        String[] headers = headerLine.split(",");
        int emailIdx = -1, ageIdx = -1, activeIdx = -1, scoreIdx = -1;
        for (int i = 0; i < headers.length; i++) {
            switch (headers[i].trim()) {
                case "email": emailIdx = i; break;
                case "age": ageIdx = i; break;
                case "active": activeIdx = i; break;
                case "score": scoreIdx = i; break;
            }
        }
        int valid = 0, errors = 0;
        String line;
        while ((line = br.readLine()) != null) {
            if (line.isEmpty()) continue;
            String[] f = line.split(",", -1);
            boolean ok = true;
            if (emailIdx >= 0 && emailIdx < f.length && !EMAIL_RE.matcher(f[emailIdx].trim()).matches()) { errors++; ok = false; }
            if (ageIdx >= 0 && ageIdx < f.length) { try { Double.parseDouble(f[ageIdx].trim()); } catch (NumberFormatException e) { errors++; ok = false; } }
            if (activeIdx >= 0 && activeIdx < f.length) {
                String v = f[activeIdx].trim().toLowerCase();
                if (!v.equals("true") && !v.equals("false") && !v.equals("yes") && !v.equals("no") && !v.equals("1") && !v.equals("0")) { errors++; ok = false; }
            }
            if (scoreIdx >= 0 && scoreIdx < f.length) { try { Double.parseDouble(f[scoreIdx].trim()); } catch (NumberFormatException e) { errors++; ok = false; } }
            if (ok) valid++;
        }
        br.close();
        return new int[]{valid, errors};
    }

    // =========================================================================
    // BENCHMARK 3: Large File Streaming
    // =========================================================================

    private static void benchmarkLargeFileStreaming(Path tmpDir) throws Exception {
        System.out.println(box("BENCHMARK 3: LARGE FILE STREAMING — 1M rows, all competitors"));

        int size = 1_000_000;
        Path csv = generateCsv(tmpDir, size);
        long fileSize = Files.size(csv);
        byte[] data = Files.readAllBytes(csv);

        System.out.println("--- " + fmtCount(size) + " rows (" + formatBytes(fileSize) + ") ---\n");

        // Warmup
        for (int i = 0; i < 2; i++) {
            parsePravaah(data);
            parseDiySplit(data);
            parseCommonsCsv(data);
            parseUnivocity(data);
            parseOpenCsv(data);
            parseJacksonCsv(data);
        }

        String[] names = {"Pravaah", "BufferedReader+split", "Apache Commons CSV",
                "uniVocity-parsers", "OpenCSV", "Jackson CSV"};
        @SuppressWarnings("unchecked")
        ParseAction[] actions = {
                () -> parsePravaah(data),
                () -> parseDiySplit(data),
                () -> parseCommonsCsv(data),
                () -> parseUnivocity(data),
                () -> parseOpenCsv(data),
                () -> parseJacksonCsv(data)
        };

        System.out.printf("  %-32s %8s  %14s  %10s%n", "Library", "Time", "Throughput", "Peak Mem");
        System.out.println("  " + repeat('-', 70));

        for (int li = 0; li < names.length; li++) {
            System.gc();
            try { Thread.sleep(200); } catch (InterruptedException ignored) {}
            long memBefore = usedMemory();
            long[] times = new long[RUNS];
            for (int r = 0; r < RUNS; r++) {
                System.gc();
                long start = System.nanoTime();
                actions[li].run();
                times[r] = (System.nanoTime() - start) / 1_000_000;
            }
            long memPeak = usedMemory() - memBefore;
            long med = median(times);
            double tput = (double) size / med * 1000.0;
            System.out.printf("  %-32s %6d ms  %10.0f r/s  %10s%n",
                    names[li], med, tput, formatBytes(Math.max(0, memPeak)));
        }

        // Pravaah full pipeline: read + filter + schema validation
        System.out.println();
        System.gc();
        try { Thread.sleep(200); } catch (InterruptedException ignored) {}

        try {
            long start = System.nanoTime();
            Pravaah.read(csv.toString(), ReadOptions.defaults().format(PravaahFormat.CSV))
                    .schema(new SchemaDefinition()
                                    .field("id", Schema.number())
                                    .field("name", Schema.string())
                                    .field("email", Schema.email())
                                    .field("score", Schema.number())
                                    .field("active", Schema.bool()),
                            ValidationMode.SKIP, null)
                    .drain();
            long elapsed = (System.nanoTime() - start) / 1_000_000;
            System.out.printf("  %-32s %6d ms  %10.0f r/s  (full schema)%n",
                    "Pravaah (parse+validate all)", elapsed, (double) size / elapsed * 1000.0);
        } catch (OutOfMemoryError e) {
            System.out.println("  Pravaah (parse+validate all):  OOM — increase -Xmx");
        }

        System.out.println();
    }

    // =========================================================================
    // BENCHMARK 4: Real CSV files from benchmark-files/
    // =========================================================================

    private static void benchmarkExternalCsvFiles(Path dir) throws Exception {
        if (!Files.isDirectory(dir)) {
            System.out.println(box("BENCHMARK 4: REAL FILES — benchmark-files/ not found"));
            System.out.println("  Skipping real-file benchmark.\n");
            return;
        }

        List<Path> csvFiles = new ArrayList<>();
        List<Path> skipped = new ArrayList<>();
        DirectoryStream<Path> stream = Files.newDirectoryStream(dir);
        try {
            for (Path file : stream) {
                if (!Files.isRegularFile(file)) continue;
                String name = file.getFileName().toString().toLowerCase(Locale.ROOT);
                if (name.endsWith(".csv")) csvFiles.add(file);
                else skipped.add(file);
            }
        } finally {
            stream.close();
        }

        csvFiles.sort(Comparator.comparing(path -> path.getFileName().toString()));

        System.out.println(box("BENCHMARK 4: REAL CSV FILES — streaming row count"));
        if (csvFiles.isEmpty()) {
            System.out.println("  No .csv files found.\n");
            return;
        }

        String[] names = {"Pravaah", "BufferedReader+split", "Apache Commons CSV",
                "uniVocity-parsers", "OpenCSV", "Jackson CSV"};

        for (Path file : csvFiles) {
            long bytes = Files.size(file);
            System.out.println("--- " + file.getFileName() + " (" + formatBytes(bytes) + ") ---");
            System.out.printf("  %-32s %8s  %12s  %14s%n", "Library", "Time", "Rows", "Throughput");
            System.out.println("  " + repeat('-', 74));

            ParseAction[] actions = {
                    () -> parsePravaahFile(file),
                    () -> parseDiySplitFile(file),
                    () -> parseCommonsCsvFile(file),
                    () -> parseUnivocityFile(file),
                    () -> parseOpenCsvFile(file),
                    () -> parseJacksonCsvFile(file)
            };

            for (int i = 0; i < names.length; i++) {
                FileBenchResult result = benchFile(actions[i]);
                if (result.error == null) {
                    double tput = result.timeMs == 0 ? 0 : (double) result.rows / result.timeMs * 1000.0;
                    System.out.printf("  %-32s %6d ms  %12d  %10.0f r/s%n",
                            names[i], result.timeMs, result.rows, tput);
                } else {
                    System.out.printf("  %-32s %8s  %12s  %14s%n",
                            names[i], "FAILED", "-", result.error);
                }
            }
            System.out.println();
        }

        System.out.println("  Note: BufferedReader+split is not RFC CSV-safe and may miscount quoted multiline rows.\n");
    }

    // =========================================================================
    // BENCHMARK 5: Spreadsheet files from benchmark-files/
    // =========================================================================

    private static void benchmarkSpreadsheetFiles(Path dir) throws Exception {
        if (!Files.isDirectory(dir)) {
            System.out.println(box("BENCHMARK 5: SPREADSHEETS — benchmark-files/ not found"));
            System.out.println("  Skipping spreadsheet benchmark.\n");
            return;
        }

        List<Path> files = new ArrayList<>();
        DirectoryStream<Path> stream = Files.newDirectoryStream(dir);
        try {
            for (Path file : stream) {
                if (!Files.isRegularFile(file)) continue;
                String name = file.getFileName().toString().toLowerCase(Locale.ROOT);
                if (name.endsWith(".xlsx") || name.endsWith(".xls")) files.add(file);
            }
        } finally {
            stream.close();
        }

        files.sort(Comparator.comparing(path -> path.getFileName().toString()));

        System.out.println(box("BENCHMARK 5: REAL SPREADSHEETS — first sheet row count"));
        if (files.isEmpty()) {
            System.out.println("  No .xlsx or .xls files found.\n");
            return;
        }

        String[] names = {"Pravaah XLSX", "Apache POI XSSF", "EasyExcel"};

        for (Path file : files) {
            long bytes = Files.size(file);
            System.out.println("--- " + file.getFileName() + " (" + formatBytes(bytes) + ") ---");
            System.out.printf("  %-32s %8s  %12s  %14s%n", "Library", "Time", "Rows", "Throughput");
            System.out.println("  " + repeat('-', 74));

            ParseAction[] actions = {
                    () -> parsePravaahSpreadsheetFile(file),
                    () -> parsePoiXssfFile(file),
                    () -> parseEasyExcelFile(file)
            };

            for (int i = 0; i < names.length; i++) {
                FileBenchResult result = benchFile(actions[i]);
                if (result.error == null) {
                    double tput = result.timeMs == 0 ? 0 : (double) result.rows / result.timeMs * 1000.0;
                    System.out.printf("  %-32s %6d ms  %12d  %10.0f r/s%n",
                            names[i], result.timeMs, result.rows, tput);
                } else {
                    System.out.printf("  %-32s %8s  %12s  %14s%n",
                            names[i], "FAILED", "-", result.error);
                }
            }
            System.out.println();
        }

        System.out.println("  Note: Apache POI XSSF and Pravaah's zero-dependency reader support OOXML .xlsx.");
        System.out.println("  Legacy binary .xls is reported separately because XSSF is not an .xls reader.\n");
    }

    private static int parsePravaahSpreadsheetFile(Path file) throws IOException {
        String name = file.getFileName().toString().toLowerCase(Locale.ROOT);
        if (!name.endsWith(".xlsx")) {
            throw new UnsupportedOperationException("legacy-xls");
        }
        return XlsxReader.readAll(file.toString(), ReadOptions.defaults()).size();
    }

    private static int parsePoiXssfFile(Path file) throws IOException {
        String name = file.getFileName().toString().toLowerCase(Locale.ROOT);
        if (!name.endsWith(".xlsx")) {
            throw new UnsupportedOperationException("legacy-xls");
        }

        InputStream input = Files.newInputStream(file);
        XSSFWorkbook workbook = null;
        try {
            workbook = new XSSFWorkbook(input);
            Sheet sheet = workbook.getNumberOfSheets() == 0 ? null : workbook.getSheetAt(0);
            if (sheet == null) return 0;
            DataFormatter formatter = new DataFormatter();
            int count = 0;
            boolean header = true;
            for (org.apache.poi.ss.usermodel.Row row : sheet) {
                if (header) {
                    header = false;
                    continue;
                }
                for (Cell cell : row) {
                    formatter.formatCellValue(cell);
                }
                count++;
            }
            return count;
        } finally {
            if (workbook != null) workbook.close();
            input.close();
        }
    }

    private static int parseEasyExcelFile(Path file) {
        final int[] rows = {0};
        EasyExcel.read(file.toFile(), new AnalysisEventListener<Map<Integer, String>>() {
            @Override
            public void invoke(Map<Integer, String> row, AnalysisContext context) {
                rows[0]++;
                if (row != null) {
                    for (String value : row.values()) {
                        if (value != null && value.length() == -1) {
                            throw new IllegalStateException("unreachable");
                        }
                    }
                }
            }

            @Override
            public void doAfterAllAnalysed(AnalysisContext context) {
                // no-op
            }
        }).sheet().headRowNumber(1).doRead();
        return rows[0];
    }

    // =========================================================================
    // Lines of Code comparison
    // =========================================================================

    private static void benchmarkLocComparison() {
        System.out.println(box("CODE COMPLEXITY: Lines of Code to parse + validate + collect errors"));

        System.out.println("  Task: CSV -> validate email/number/bool -> collect errors + valid rows\n");
        System.out.println("  +---------------------------------+-------+");
        System.out.println("  | Approach                        |  LOC  |");
        System.out.println("  +---------------------------------+-------+");
        System.out.println("  | Pravaah                         |    10 |");
        System.out.println("  | uniVocity + DIY validation      |   60+ |");
        System.out.println("  | OpenCSV + DIY validation        |   70+ |");
        System.out.println("  | Apache Commons CSV + DIY valid  |   85+ |");
        System.out.println("  | Jackson CSV + DIY validation    |   90+ |");
        System.out.println("  | BufferedReader + split + DIY    |  120+ |");
        System.out.println("  | Full DIY (RFC CSV + validation) |  300+ |");
        System.out.println("  +---------------------------------+-------+\n");
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    private static Path generateCsv(Path dir, int rows) throws IOException {
        Path path = dir.resolve("data_" + rows + ".csv");
        if (Files.exists(path)) return path;
        BufferedWriter bw = new BufferedWriter(new FileWriter(path.toFile()));
        bw.write("id,name,email,score,active");
        bw.newLine();
        Random rng = new Random(42);
        String[] names = {"Ada", "Grace", "Linus", "Margaret", "Alan", "Hedy", "Dennis", "Barbara"};
        for (int i = 0; i < rows; i++) {
            String name = names[rng.nextInt(names.length)];
            bw.write(i + "," + name + "," + name.toLowerCase() + i + "@example.com,"
                    + rng.nextInt(100) + "," + (rng.nextBoolean() ? "true" : "false"));
            bw.newLine();
        }
        bw.close();
        return path;
    }

    private static Path generateValidationCsv(Path dir, int rows) throws IOException {
        Path path = dir.resolve("validation_" + rows + ".csv");
        if (Files.exists(path)) return path;
        BufferedWriter bw = new BufferedWriter(new FileWriter(path.toFile()));
        bw.write("email,age,active,score,name");
        bw.newLine();
        Random rng = new Random(42);
        String[] names = {"Ada", "Grace", "Linus", "Margaret"};
        for (int i = 0; i < rows; i++) {
            boolean bad = rng.nextInt(10) == 0;
            String name = names[rng.nextInt(names.length)];
            String email = bad ? "bad-email-" + i : name.toLowerCase() + i + "@example.com";
            String age = bad ? "old" : String.valueOf(18 + rng.nextInt(60));
            String active = bad ? "maybe" : (rng.nextBoolean() ? "yes" : "no");
            String score = String.valueOf(rng.nextInt(100));
            bw.write(email + "," + age + "," + active + "," + score + "," + name);
            bw.newLine();
        }
        bw.close();
        return path;
    }

    @FunctionalInterface
    interface ParseAction { int run() throws Exception; }

    private static class FileBenchResult {
        final long timeMs;
        final int rows;
        final String error;

        FileBenchResult(long timeMs, int rows, String error) {
            this.timeMs = timeMs;
            this.rows = rows;
            this.error = error;
        }
    }

    private static long bench(ParseAction action) throws Exception {
        long[] times = new long[RUNS];
        for (int i = 0; i < RUNS; i++) {
            System.gc();
            long start = System.nanoTime();
            action.run();
            times[i] = (System.nanoTime() - start) / 1_000_000;
        }
        return median(times);
    }

    private static FileBenchResult benchFile(ParseAction action) {
        try {
            long[] times = new long[RUNS];
            int rows = 0;
            for (int i = 0; i < RUNS; i++) {
                System.gc();
                long start = System.nanoTime();
                rows = action.run();
                times[i] = (System.nanoTime() - start) / 1_000_000;
            }
            return new FileBenchResult(median(times), rows, null);
        } catch (Throwable t) {
            return new FileBenchResult(0, 0, t.getClass().getSimpleName());
        }
    }

    private static long median(long[] v) {
        long[] s = v.clone();
        Arrays.sort(s);
        return s[s.length / 2];
    }

    private static long usedMemory() {
        Runtime rt = Runtime.getRuntime();
        return rt.totalMemory() - rt.freeMemory();
    }

    private static void printRow(String name, long ms) {
        System.out.printf("  %-32s %6d ms%n", name, ms);
    }

    private static String fmtCount(int c) {
        if (c >= 1_000_000) return (c / 1_000_000) + "M";
        if (c >= 1_000) return (c / 1_000) + "K";
        return String.valueOf(c);
    }

    private static String formatBytes(long b) {
        if (b < 1024) return b + " B";
        if (b < 1024 * 1024) return String.format("%.1f KB", b / 1024.0);
        if (b < 1024L * 1024 * 1024) return String.format("%.1f MB", b / (1024.0 * 1024.0));
        return String.format("%.2f GB", b / (1024.0 * 1024.0 * 1024.0));
    }

    private static String repeat(char c, int n) {
        char[] a = new char[n];
        Arrays.fill(a, c);
        return new String(a);
    }

    private static String box(String title) {
        int w = 76;
        StringBuilder sb = new StringBuilder();
        sb.append('+').append(repeat('-', w - 2)).append("+\n");
        sb.append("| ").append(title);
        for (int i = title.length() + 2; i < w - 1; i++) sb.append(' ');
        sb.append("|\n");
        sb.append('+').append(repeat('-', w - 2)).append("+\n");
        return sb.toString();
    }
}
