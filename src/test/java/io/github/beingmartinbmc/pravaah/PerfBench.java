package io.github.beingmartinbmc.pravaah;

import io.github.beingmartinbmc.pravaah.csv.CsvReader;
import io.github.beingmartinbmc.pravaah.csv.CsvWriter;
import io.github.beingmartinbmc.pravaah.xls.XlsReader;
import io.github.beingmartinbmc.pravaah.xlsx.XlsxReader;
import io.github.beingmartinbmc.pravaah.xlsx.XlsxWriter;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Performance benchmark mirroring the workloads documented in the README.
 *
 * Reads the actual benchmark files in {@code benchmark-files/} and writes a
 * synthetic 100,000 x 10 row dataset for write benchmarks. Reports the best
 * observed time after warmup so results match the methodology described in
 * the README.
 *
 * Run:
 *   javac -d target/test-classes -cp target/classes \
 *       src/test/java/io/github/beingmartinbmc/pravaah/PerfBench.java
 *   java -Xmx6g -cp target/classes:target/test-classes \
 *       io.github.beingmartinbmc.pravaah.PerfBench
 */
public final class PerfBench {

    private static final Path FILES = Paths.get("benchmark-files");
    private static final Path OUT = Paths.get("target", "perf-bench");

    public static void main(String[] args) throws Exception {
        Files.createDirectories(OUT);
        System.out.println("===============================================================");
        System.out.printf("Pravaah PerfBench  |  JVM %s  |  cores=%d  |  -Xmx=%s%n",
                System.getProperty("java.version"),
                Runtime.getRuntime().availableProcessors(),
                fmtBytes(Runtime.getRuntime().maxMemory()));
        System.out.println("===============================================================\n");

        readCsvCount("MOCK_DATA.csv");
        readCsvCount("Crime_Data_from_2020_to_2024.csv");
        readCsvCount("geographic-units-by-industry-and-statistical-area-2000-2025-descending-order-february-2025.csv");

        readXlsx("MOCK_DATA.xlsx");
        readXlsx("hts_2024_revision_9_xlsx.xlsx");

        readXls("geographic-units-by-industry-and-statistical-area-2000-2025-descending-order-february-2025 copy.xls");

        writeCsv();
        writeXlsx();

        System.out.println("---------------------------------------------------------------");
    }

    // -------------------- CSV read --------------------

    private static void readCsvCount(String name) throws Exception {
        Path file = FILES.resolve(name);
        if (!Files.exists(file)) {
            System.out.printf("SKIP  csv-count  %-60s (missing)%n", name);
            return;
        }

        ReadOptions options = ReadOptions.defaults().format(PravaahFormat.CSV);
        for (int i = 0; i < 2; i++) CsvReader.drainCount(file.toString(), options);

        long bestNs = Long.MAX_VALUE;
        int rows = 0;
        for (int i = 0; i < 4; i++) {
            long t = System.nanoTime();
            rows = CsvReader.drainCount(file.toString(), options);
            long el = System.nanoTime() - t;
            if (el < bestNs) bestNs = el;
        }
        report("csv-count", name, Files.size(file), rows, bestNs);
    }

    // -------------------- XLSX read --------------------

    private static void readXlsx(String name) throws Exception {
        Path file = FILES.resolve(name);
        if (!Files.exists(file)) {
            System.out.printf("SKIP  xlsx       %-60s (missing)%n", name);
            return;
        }

        ReadOptions options = ReadOptions.defaults();
        byte[] bytes = Files.readAllBytes(file);
        for (int i = 0; i < 2; i++) XlsxReader.readAll(bytes, options);

        long bestNs = Long.MAX_VALUE;
        int rows = 0;
        for (int i = 0; i < 4; i++) {
            long t = System.nanoTime();
            List<Row> r = XlsxReader.readAll(bytes, options);
            long el = System.nanoTime() - t;
            rows = r.size();
            if (el < bestNs) bestNs = el;
        }
        report("xlsx-read", name, Files.size(file), rows, bestNs);
    }

    // -------------------- XLS read --------------------

    private static void readXls(String name) throws Exception {
        Path file = FILES.resolve(name);
        if (!Files.exists(file)) {
            System.out.printf("SKIP  xls        %-60s (missing)%n", name);
            return;
        }

        ReadOptions options = ReadOptions.defaults();
        byte[] bytes = Files.readAllBytes(file);
        for (int i = 0; i < 2; i++) XlsReader.readAll(bytes, options);

        long bestNs = Long.MAX_VALUE;
        int rows = 0;
        for (int i = 0; i < 4; i++) {
            long t = System.nanoTime();
            List<Row> r = XlsReader.readAll(bytes, options);
            long el = System.nanoTime() - t;
            rows = r.size();
            if (el < bestNs) bestNs = el;
        }
        report("xls-read", name, Files.size(file), rows, bestNs);
    }

    // -------------------- CSV write --------------------

    private static void writeCsv() throws Exception {
        List<Row> rows = generateRows(100_000, 10);
        File out = OUT.resolve("write.csv").toFile();
        WriteOptions options = WriteOptions.defaults().format(PravaahFormat.CSV);

        for (int i = 0; i < 2; i++) CsvWriter.write(rows, out.getAbsolutePath(), options);

        long bestNs = Long.MAX_VALUE;
        for (int i = 0; i < 4; i++) {
            long t = System.nanoTime();
            CsvWriter.write(rows, out.getAbsolutePath(), options);
            long el = System.nanoTime() - t;
            if (el < bestNs) bestNs = el;
        }
        report("csv-write", "100,000 x 10 synthetic", out.length(), rows.size(), bestNs);
    }

    // -------------------- XLSX write --------------------

    private static void writeXlsx() throws Exception {
        List<Row> rows = generateRows(100_000, 10);
        File out = OUT.resolve("write.xlsx").toFile();
        WriteOptions options = WriteOptions.defaults().format(PravaahFormat.XLSX);

        for (int i = 0; i < 2; i++) XlsxWriter.writeRows(rows, out.getAbsolutePath(), options);

        long bestNs = Long.MAX_VALUE;
        for (int i = 0; i < 4; i++) {
            long t = System.nanoTime();
            XlsxWriter.writeRows(rows, out.getAbsolutePath(), options);
            long el = System.nanoTime() - t;
            if (el < bestNs) bestNs = el;
        }
        report("xlsx-write", "100,000 x 10 synthetic", out.length(), rows.size(), bestNs);
    }

    // -------------------- helpers --------------------

    private static List<Row> generateRows(int count, int cols) {
        String[] headers = new String[cols];
        for (int c = 0; c < cols; c++) headers[c] = "col" + (c + 1);

        List<Row> rows = new ArrayList<>(count);
        for (int r = 0; r < count; r++) {
            Map<String, Object> map = new LinkedHashMap<>(cols * 2);
            for (int c = 0; c < cols; c++) {
                if ((c & 1) == 0) {
                    map.put(headers[c], "value-" + r + "-" + c);
                } else {
                    map.put(headers[c], r * 31L + c);
                }
            }
            rows.add(new Row(map));
        }
        return rows;
    }

    private static void report(String op, String name, long bytes, int rows, long bestNs) {
        double ms = bestNs / 1_000_000.0;
        System.out.printf("%-12s %-65s %10s  rows=%-10d  best=%.1f ms%n",
                op, truncate(name, 65), fmtBytes(bytes), rows, ms);
    }

    private static String truncate(String s, int max) {
        return s.length() <= max ? s : s.substring(0, max - 1) + "…";
    }

    private static String fmtBytes(long bytes) {
        if (bytes < 1024) return bytes + " B";
        double kb = bytes / 1024.0;
        if (kb < 1024) return String.format("%.1f KB", kb);
        double mb = kb / 1024.0;
        if (mb < 1024) return String.format("%.1f MB", mb);
        return String.format("%.2f GB", mb / 1024.0);
    }
}
