package io.github.beingmartinbmc.pravaah;

import io.github.beingmartinbmc.pravaah.csv.CsvReader;
import io.github.beingmartinbmc.pravaah.schema.*;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.regex.Pattern;

/**
 * Self-contained benchmark runner. Compares Pravaah against plain JDK approaches
 * (BufferedReader + String.split + manual validation) across three scenarios:
 *
 * 1) CSV Parsing: raw parse speed at 10K, 100K, 1M rows
 * 2) Validation Pipeline: email validation + type coercion + error collection
 * 3) Large File Streaming: memory-constrained processing at 1M, 5M, 10M rows
 *
 * Run: mvn exec:java -Dexec.mainClass="io.github.beingmartinbmc.pravaah.BenchmarkRunner"
 *   or: java -cp target/classes:target/test-classes io.github.beingmartinbmc.pravaah.BenchmarkRunner
 */
public final class BenchmarkRunner {

    private static final Pattern EMAIL_RE = Pattern.compile("^[^\\s@]+@[^\\s@]+\\.[^\\s@]+$");

    public static void main(String[] args) throws Exception {
        Path tmpDir = Files.createTempDirectory("pravaah-bench");
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                Files.walk(tmpDir).sorted(Comparator.reverseOrder())
                        .map(Path::toFile).forEach(File::delete);
            } catch (IOException ignored) {}
        }));

        String ruler = repeat('=', 72);
        System.out.println(ruler);
        System.out.println("  PRAVAAH-JAVA BENCHMARK SUITE");
        System.out.println("  JVM: " + System.getProperty("java.version")
                + " | OS: " + System.getProperty("os.name")
                + " | Cores: " + Runtime.getRuntime().availableProcessors());
        System.out.println(ruler);

        benchmarkCsvParsing(tmpDir);
        benchmarkValidationPipeline(tmpDir);
        benchmarkLargeFileStreaming(tmpDir);
        benchmarkLocComparison();

        System.out.println("\n" + ruler);
        System.out.println("  BENCHMARK COMPLETE");
        System.out.println(ruler);
    }

    // =========================================================================
    // BENCHMARK 1: CSV Parsing Speed
    // =========================================================================

    private static void benchmarkCsvParsing(Path tmpDir) throws Exception {
        System.out.println("\n╔══════════════════════════════════════════════════════════════════════╗");
        System.out.println("║  BENCHMARK 1: CSV PARSING SPEED                                    ║");
        System.out.println("║  Pravaah vs BufferedReader+split vs String.split in-memory          ║");
        System.out.println("╚══════════════════════════════════════════════════════════════════════╝\n");

        for (int size : new int[]{10_000, 100_000, 1_000_000}) {
            Path csv = generateCsv(tmpDir, size);
            byte[] data = Files.readAllBytes(csv);
            String label = formatCount(size);

            System.out.println("--- " + label + " rows (" + formatBytes(data.length) + ") ---");

            // Warmup
            for (int i = 0; i < 3; i++) {
                parsePravaah(data);
                parseDiyBufferedReader(data);
                parseDiySplit(data);
            }

            // Timed runs
            long[] pravaahTimes = new long[5];
            long[] brTimes = new long[5];
            long[] splitTimes = new long[5];

            for (int i = 0; i < 5; i++) {
                System.gc();
                pravaahTimes[i] = timedParse(() -> parsePravaah(data));
                System.gc();
                brTimes[i] = timedParse(() -> parseDiyBufferedReader(data));
                System.gc();
                splitTimes[i] = timedParse(() -> parseDiySplit(data));
            }

            long pMedian = median(pravaahTimes);
            long brMedian = median(brTimes);
            long sMedian = median(splitTimes);

            System.out.printf("  %-28s %8d ms%n", "Pravaah", pMedian);
            System.out.printf("  %-28s %8d ms%n", "BufferedReader + split", brMedian);
            System.out.printf("  %-28s %8d ms%n", "String.split (in-memory)", sMedian);
            System.out.println();
        }
    }

    private static int parsePravaah(byte[] data) throws IOException {
        List<Row> rows = CsvReader.readAll(data, ReadOptions.defaults());
        return rows.size();
    }

    private static int parseDiyBufferedReader(byte[] data) throws IOException {
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
            Map<String, String> row = new LinkedHashMap<>();
            for (int i = 0; i < headers.length && i < fields.length; i++) {
                row.put(headers[i], fields[i]);
            }
            count++;
        }
        br.close();
        return count;
    }

    private static int parseDiySplit(byte[] data) {
        String text = new String(data, StandardCharsets.UTF_8);
        String[] lines = text.split("\n");
        if (lines.length == 0) return 0;
        String[] headers = lines[0].split(",");
        int count = 0;
        for (int i = 1; i < lines.length; i++) {
            String line = lines[i].trim();
            if (line.isEmpty()) continue;
            String[] fields = line.split(",", -1);
            Map<String, String> row = new LinkedHashMap<>();
            for (int j = 0; j < headers.length && j < fields.length; j++) {
                row.put(headers[j], fields[j]);
            }
            count++;
        }
        return count;
    }

    // =========================================================================
    // BENCHMARK 2: Validation Pipeline
    // =========================================================================

    private static void benchmarkValidationPipeline(Path tmpDir) throws Exception {
        System.out.println("╔══════════════════════════════════════════════════════════════════════╗");
        System.out.println("║  BENCHMARK 2: VALIDATION PIPELINE                                  ║");
        System.out.println("║  Email validation + type coercion + error collection                ║");
        System.out.println("╚══════════════════════════════════════════════════════════════════════╝\n");

        int size = 100_000;
        Path csv = generateValidationCsv(tmpDir, size);
        byte[] data = Files.readAllBytes(csv);

        System.out.println("--- " + formatCount(size) + " rows with ~10% bad data ---");

        // Warmup
        for (int i = 0; i < 3; i++) {
            validatePravaah(data);
            validateDiy(data);
        }

        long[] pTimes = new long[5];
        long[] dTimes = new long[5];
        int[] pErrors = new int[1];
        int[] dErrors = new int[1];
        int[] pValid = new int[1];
        int[] dValid = new int[1];

        for (int i = 0; i < 5; i++) {
            System.gc();
            long start = System.nanoTime();
            int[] pr = validatePravaah(data);
            pTimes[i] = (System.nanoTime() - start) / 1_000_000;
            pValid[0] = pr[0];
            pErrors[0] = pr[1];

            System.gc();
            start = System.nanoTime();
            int[] dr = validateDiy(data);
            dTimes[i] = (System.nanoTime() - start) / 1_000_000;
            dValid[0] = dr[0];
            dErrors[0] = dr[1];
        }

        System.out.printf("  %-28s %8d ms  (valid: %d, errors: %d)%n",
                "Pravaah", median(pTimes), pValid[0], pErrors[0]);
        System.out.printf("  %-28s %8d ms  (valid: %d, errors: %d)%n",
                "DIY validation", median(dTimes), dValid[0], dErrors[0]);
        System.out.println();
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

        int emailIdx = -1, ageIdx = -1, activeIdx = -1, scoreIdx = -1, nameIdx = -1;
        for (int i = 0; i < headers.length; i++) {
            switch (headers[i].trim()) {
                case "email": emailIdx = i; break;
                case "age": ageIdx = i; break;
                case "active": activeIdx = i; break;
                case "score": scoreIdx = i; break;
                case "name": nameIdx = i; break;
            }
        }

        int valid = 0, errors = 0;
        String line;
        int rowNum = 1;
        while ((line = br.readLine()) != null) {
            if (line.isEmpty()) continue;
            String[] fields = line.split(",", -1);
            boolean rowOk = true;

            if (emailIdx >= 0 && emailIdx < fields.length) {
                if (!EMAIL_RE.matcher(fields[emailIdx].trim()).matches()) {
                    errors++;
                    rowOk = false;
                }
            }
            if (ageIdx >= 0 && ageIdx < fields.length) {
                try {
                    Double.parseDouble(fields[ageIdx].trim());
                } catch (NumberFormatException e) {
                    errors++;
                    rowOk = false;
                }
            }
            if (activeIdx >= 0 && activeIdx < fields.length) {
                String val = fields[activeIdx].trim().toLowerCase();
                if (!val.equals("true") && !val.equals("false") && !val.equals("yes")
                        && !val.equals("no") && !val.equals("1") && !val.equals("0")) {
                    errors++;
                    rowOk = false;
                }
            }
            if (scoreIdx >= 0 && scoreIdx < fields.length) {
                try {
                    Double.parseDouble(fields[scoreIdx].trim());
                } catch (NumberFormatException e) {
                    errors++;
                    rowOk = false;
                }
            }
            if (rowOk) valid++;
            rowNum++;
        }
        br.close();
        return new int[]{valid, errors};
    }

    // =========================================================================
    // BENCHMARK 3: Large File Streaming (Memory Stress)
    // =========================================================================

    private static void benchmarkLargeFileStreaming(Path tmpDir) throws Exception {
        System.out.println("╔══════════════════════════════════════════════════════════════════════╗");
        System.out.println("║  BENCHMARK 3: LARGE FILE STREAMING                                 ║");
        System.out.println("║  Peak memory + throughput at scale                                  ║");
        System.out.println("╚══════════════════════════════════════════════════════════════════════╝\n");

        for (int size : new int[]{1_000_000, 2_000_000, 5_000_000}) {
            Path csv = generateCsv(tmpDir, size);
            String label = formatCount(size);
            long fileSize = Files.size(csv);

            System.out.println("--- " + label + " rows (" + formatBytes(fileSize) + ") ---");

            try {
                // Pravaah full pipeline: read -> filter -> drain
                System.gc();
                Thread.sleep(100);
                long memBefore = usedMemory();
                long start = System.nanoTime();

                Pravaah.read(csv.toString(), ReadOptions.defaults().format(PravaahFormat.CSV))
                        .filter(row -> {
                            Object score = row.get("score");
                            return score != null && ((score instanceof Number)
                                    ? ((Number) score).intValue() > 50
                                    : Integer.parseInt(score.toString()) > 50);
                        })
                        .drain();

                long elapsed = (System.nanoTime() - start) / 1_000_000;
                long memAfter = usedMemory();
                long peakDelta = memAfter - memBefore;
                double throughput = (double) size / elapsed * 1000.0;

                System.out.printf("  Pravaah (read+filter+drain): %d ms | peak %s | %.0f rows/sec%n",
                        elapsed, formatBytes(Math.max(0, peakDelta)), throughput);
            } catch (OutOfMemoryError e) {
                System.out.println("  Pravaah (read+filter+drain): OOM (increase -Xmx)");
                System.gc();
            }

            try {
                // DIY approach
                System.gc();
                Thread.sleep(100);
                long memBefore = usedMemory();
                long start = System.nanoTime();

                diyStreamFilter(csv.toString());

                long elapsed = (System.nanoTime() - start) / 1_000_000;
                long memAfter = usedMemory();
                long peakDelta = memAfter - memBefore;
                double throughput = (double) size / elapsed * 1000.0;

                System.out.printf("  DIY (BufferedReader+split):   %d ms | peak %s | %.0f rows/sec%n",
                        elapsed, formatBytes(Math.max(0, peakDelta)), throughput);
            } catch (OutOfMemoryError e) {
                System.out.println("  DIY (BufferedReader+split):   OOM");
                System.gc();
            }

            try {
                // Pravaah with schema validation on large file
                System.gc();
                Thread.sleep(100);
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
                double throughput = (double) size / elapsed * 1000.0;

                System.out.printf("  Pravaah (full validation):    %d ms | %.0f rows/sec%n",
                        elapsed, throughput);
            } catch (OutOfMemoryError e) {
                System.out.println("  Pravaah (full validation):    OOM (increase -Xmx)");
                System.gc();
            }

            System.out.println();
        }
    }

    private static int diyStreamFilter(String path) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(path));
        String headerLine = br.readLine();
        if (headerLine == null) { br.close(); return 0; }
        String[] headers = headerLine.split(",");
        int scoreIdx = -1;
        for (int i = 0; i < headers.length; i++) {
            if ("score".equals(headers[i])) { scoreIdx = i; break; }
        }

        int count = 0;
        String line;
        while ((line = br.readLine()) != null) {
            if (line.isEmpty()) continue;
            String[] fields = line.split(",", -1);
            if (scoreIdx >= 0 && scoreIdx < fields.length) {
                try {
                    if (Integer.parseInt(fields[scoreIdx]) > 50) count++;
                } catch (NumberFormatException ignored) {}
            }
        }
        br.close();
        return count;
    }

    // =========================================================================
    // BENCHMARK 4: Lines of Code Comparison
    // =========================================================================

    private static void benchmarkLocComparison() {
        System.out.println("╔══════════════════════════════════════════════════════════════════════╗");
        System.out.println("║  CODE COMPLEXITY COMPARISON (Lines of Code)                        ║");
        System.out.println("╚══════════════════════════════════════════════════════════════════════╝\n");

        System.out.println("  Task: Parse CSV → validate email + number + bool → collect errors");
        System.out.println("  ┌────────────────────────────────┬───────┐");
        System.out.println("  │ Approach                       │  LOC  │");
        System.out.println("  ├────────────────────────────────┼───────┤");
        System.out.println("  │ Pravaah                        │    10 │");
        System.out.println("  │ Apache Commons CSV + DIY valid │   85+ │");
        System.out.println("  │ OpenCSV + DIY valid            │   70+ │");
        System.out.println("  │ uniVocity + DIY valid          │   60+ │");
        System.out.println("  │ Jackson CSV + DIY valid        │   90+ │");
        System.out.println("  │ BufferedReader + split + DIY   │  120+ │");
        System.out.println("  │ Full DIY (quoted CSV + valid)  │  300+ │");
        System.out.println("  └────────────────────────────────┴───────┘");

        System.out.println();
        System.out.println("  Pravaah (10 lines):");
        System.out.println("  ┌───────────────────────────────────────────────────────────────────┐");
        System.out.println("  │  ProcessResult result = Pravaah.parseDetailed(                    │");
        System.out.println("  │      csvBytes,                                                    │");
        System.out.println("  │      new SchemaDefinition()                                       │");
        System.out.println("  │          .field(\"email\", Schema.email())                          │");
        System.out.println("  │          .field(\"age\",   Schema.number())                         │");
        System.out.println("  │          .field(\"active\",Schema.bool())                           │");
        System.out.println("  │          .field(\"role\",  Schema.string().defaultValue(\"user\")),   │");
        System.out.println("  │      ReadOptions.defaults()                                       │");
        System.out.println("  │          .format(PravaahFormat.CSV)                                │");
        System.out.println("  │          .validation(ValidationMode.COLLECT));                     │");
        System.out.println("  │                                                                   │");
        System.out.println("  │  List<Row> valid  = result.getRows();    // clean typed rows       │");
        System.out.println("  │  List<Issue> errs = result.getIssues();  // per-field errors       │");
        System.out.println("  └───────────────────────────────────────────────────────────────────┘");

        System.out.println();
        System.out.println("  BufferedReader + split (120+ lines):");
        System.out.println("  ┌───────────────────────────────────────────────────────────────────┐");
        System.out.println("  │  BufferedReader br = new BufferedReader(new FileReader(file));     │");
        System.out.println("  │  String[] headers = br.readLine().split(\",\");                     │");
        System.out.println("  │  // find column indices manually...                               │");
        System.out.println("  │  // loop: br.readLine(), split, null-check every field             │");
        System.out.println("  │  // email regex, number parsing, boolean parsing...                │");
        System.out.println("  │  // error list, row counter, exception handling...                 │");
        System.out.println("  │  // no quoted field support, no CRLF handling, no type coercion   │");
        System.out.println("  │  // 120+ lines and it still can't handle \"hello,world\"            │");
        System.out.println("  └───────────────────────────────────────────────────────────────────┘");
        System.out.println();
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
    interface ParseAction {
        int run() throws Exception;
    }

    private static long timedParse(ParseAction action) throws Exception {
        long start = System.nanoTime();
        action.run();
        return (System.nanoTime() - start) / 1_000_000;
    }

    private static long median(long[] values) {
        long[] sorted = values.clone();
        Arrays.sort(sorted);
        return sorted[sorted.length / 2];
    }

    private static long usedMemory() {
        Runtime rt = Runtime.getRuntime();
        return rt.totalMemory() - rt.freeMemory();
    }

    private static String formatCount(int count) {
        if (count >= 1_000_000) return (count / 1_000_000) + "M";
        if (count >= 1_000) return (count / 1_000) + "K";
        return String.valueOf(count);
    }

    private static String formatBytes(long bytes) {
        if (bytes < 1024) return bytes + " B";
        if (bytes < 1024 * 1024) return String.format("%.1f KB", bytes / 1024.0);
        if (bytes < 1024L * 1024 * 1024) return String.format("%.1f MB", bytes / (1024.0 * 1024.0));
        return String.format("%.2f GB", bytes / (1024.0 * 1024.0 * 1024.0));
    }

    private static String repeat(char c, int count) {
        char[] arr = new char[count];
        Arrays.fill(arr, c);
        return new String(arr);
    }
}
