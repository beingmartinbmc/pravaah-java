# Pravaah Java

Stop writing CSV and Excel import code.

Pravaah is a streaming, schema-first ingestion engine for Java that turns messy CSV, XLS, XLSX, and JSON into validated, typed rows with rejection reports.

Replace hundreds of lines of parsing, validation, and cleanup logic with a single pipeline.

Looking for the Node.js version? See [`beingmartinbmc/pravaah`](https://github.com/beingmartinbmc/pravaah).

- Zero runtime dependencies
- Works on Java 8+
- Reads legacy `.xls` without Apache POI
- Faster than Apache POI on the included spreadsheet benchmarks

**Like Jackson for messy spreadsheets. ETL without Spark.**

```text
CSV direct count: 7,046,063 rows, 145MB
Pravaah Java  330ms on JDK 17

CSV read/count: 1,004,894 rows, 244MB
Pravaah Java  348ms read/count on JDK 17
```

Benchmarked locally on the repository benchmark files using JDK 8, 11, and 17 on Apple Silicon.

---

## 30-Second Win

### Before: Typical Java Import Mess

```java
BufferedReader br = new BufferedReader(new FileReader("upload.csv"));
String[] headers = br.readLine().split(",");
List<Row> rows = new ArrayList<>();
List<PravaahIssue> issues = new ArrayList<>();

String line;
int rowNumber = 1;
while ((line = br.readLine()) != null) {
    String[] cells = line.split(","); // breaks on quoted commas
    String email = find(cells, headers, "E-mail Address", "email", "mail").trim();
    if (!email.contains("@")) {
        issues.add(PravaahIssue.error("invalid_email", "Bad email", rowNumber, "email", email, "email"));
        continue;
    }
    // parse numbers, normalize booleans, handle defaults, write rejection report...
    rowNumber++;
}
```

### After: Pravaah

```java
import io.github.beingmartinbmc.pravaah.*;
import io.github.beingmartinbmc.pravaah.schema.*;

ProcessResult result = Pravaah.parseDetailed(
    "upload.csv",
    new SchemaDefinition()
        .field("email", Schema.email())
        .field("total", Schema.number())
        .field("active", Schema.bool().defaultValue(false)),
    ReadOptions.defaults()
        .format(PravaahFormat.CSV)
        .validation(ValidationMode.COLLECT)
        .cleaning(CleaningOptions.defaults()
            .trim(true)
            .fuzzyHeader("email", "E-mail", "Email Address", "mail"))
);

System.out.println(result.getRows().size() + " valid rows");
System.out.println(result.getIssues().size() + " rejected fields");
```

Typed output. Fuzzy headers. Coercion. Row-numbered issues. No `String.split`, no hand-written email regex loop, no pile of one-off import code.

---

## Why Pravaah Is Different

- No Apache POI dependency, even for `.xls`.
- One pipeline for CSV, XLS, XLSX, and JSON.
- Validation and rejection reporting are built in, not bolted on after parsing.
- CSV has a direct count path for huge files.
- Java 8 compatible, with Java 11/17 runtime overlays in the same JAR.

### Why Not Just Use Existing Libraries?

| Problem | Typical Approach | Pravaah |
| --- | --- | --- |
| CSV parsing | Commons CSV, OpenCSV, uniVocity | Built in |
| Excel reading | Apache POI | Built in, no runtime dependency |
| Schema validation | Custom code | Declarative schema |
| Header cleanup | Manual aliases | Fuzzy headers |
| Error reporting | Hand-written rejection list | Row-numbered issues |
| Multiple formats | Separate APIs | One pipeline |

---

## The Problem

You receive a spreadsheet from a customer. It might be CSV, old Excel `.xls`, modern `.xlsx`, or JSON. Headers are inconsistent, emails are invalid, numbers arrive as strings, and operations needs a rejection report.

Typical Java import code quickly becomes:

- file format detection
- parser-specific row APIs
- header mapping
- trimming and whitespace normalization
- validation
- type coercion
- duplicate handling
- error reporting

Pravaah keeps that workflow in one library.

---

## Example: Customer Upload

Input columns:

- `"E-mail Address"`
- `" Total "`
- invalid email rows
- negative or malformed totals

Pravaah output:

- normalized `email` header
- trimmed values
- typed `double` totals
- valid rows for import
- rejection report with row number, column, expected type, and raw value

```text
severity,code,message,rowNumber,column,expected,rawValue
error,invalid_type,email must be email,14,email,email,not-an-email
error,invalid_value,total cannot be negative,203,total,number,-50.00
```

---

## How It Works

```text
File: CSV/XLS/XLSX/JSON
        |
        v
Read rows -> Clean headers/values -> Validate schema -> Transform/filter -> Output/report
```

CSV uses a direct scanner that can emit to a count-only sink, row materializer, or validation sink. XLSX uses selective ZIP/XML parsing. XLS uses an internal OLE2 + BIFF8 reader, so legacy Excel reads do not require Apache POI.

---

## Install

```xml
<dependency>
    <groupId>io.github.beingmartinbmc</groupId>
    <artifactId>pravaah-java</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

Requires Java 8+. Build uses a multi-release JAR so newer JVMs automatically load newer runtime internals.

---

## Who This Is For

- Backend teams building import flows for customer spreadsheets.
- SaaS products that need validation before data enters the database.
- Batch jobs and ETL steps where bad rows need explainable rejection reports.
- Java projects that want CSV/XLS/XLSX ingestion without a runtime dependency stack.

If you need workbook editing, charts, macros, styling fidelity, pivot tables, or advanced Excel authoring, use Apache POI. Pravaah treats spreadsheets as data.

---

## When Not To Use Pravaah

- You need Excel styling, charts, macros, pivot tables, or arbitrary workbook editing. Use Apache POI.
- You need distributed processing across a cluster. Use Apache Spark or Flink.
- You only need to read one tiny CSV with no validation. Plain Java may be enough.

---

## Quick Start

### Read Any Supported File

```java
List<Row> rows = Pravaah.read("customers.xls").collect();
```

Auto-detects `.csv`, `.xls`, `.xlsx`, and `.json` from file paths. For byte arrays, pass the format:

```java
List<Row> rows = Pravaah.read(bytes, ReadOptions.defaults().format(PravaahFormat.XLS)).collect();
```

### Read A Sheet

```java
List<Row> finance = Pravaah.read("report.xlsx",
    ReadOptions.defaults().sheetName("Finance")).collect();

List<Row> second = Pravaah.read("legacy.xls",
    ReadOptions.defaults().sheetIndex(1)).collect();
```

### Transform And Write

```java
ProcessStats stats = Pravaah.read("orders.csv")
    .schema(new SchemaDefinition()
        .field("orderId", Schema.string())
        .field("email", Schema.email())
        .field("total", Schema.number()))
    .filter(row -> ((Number) row.get("total")).doubleValue() > 100)
    .write("priority-orders.xlsx", WriteOptions.defaults().format(PravaahFormat.XLSX));

System.out.println("Wrote " + stats.getRowsWritten() + " rows");
```

### Count CSV Rows Without Row Materialization

```java
int rows = CsvReader.drainCount("large.csv", ReadOptions.defaults());
```

This uses the direct CSV scanner count sink. `Pravaah.read(...).collect()` materializes rows when you need row objects.

---

## Core Focus

- CSV parser with quoted fields, escaped quotes, embedded newlines, CRLF, custom delimiters, and direct count scans.
- XLS read support through an internal OLE2/BIFF8 parser.
- XLSX read/write support through ZIP/XML parsing and writer utilities.
- JSON read/write for fixtures and pipeline output.
- Schema validation for string, number, boolean, date, email, phone, and any values.
- Cleaning with trim, whitespace normalization, fuzzy header aliases, and deduplication.
- Issue reports with row numbers, column names, expected types, and raw values.
- Lazy pipeline API with map, filter, clean, schema, take, collect, drain, process, and write.

## Advanced Features

- Formula engine, SQL-like queries, dataset diffing, joins, and plugin extension points.

---

## File Format Support

| Format | Read | Write | Notes |
| --- | :---: | :---: | --- |
| `.csv` | Yes | Yes | Direct scanner, custom delimiters, quoted records, count-only path |
| `.xls` | Yes | No | Zero-dependency OLE2/BIFF8 reader for legacy Excel workbooks |
| `.xlsx` | Yes | Yes | Selective ZIP/XML reader plus multi-sheet writer |
| `.json` | Yes | Yes | Useful for tests, snapshots, and intermediate ETL |

---

## Pipeline API

`Pravaah.read(...)` returns a lazy `PravaahPipeline`. Work runs when you call a terminal operation.

| Method | Purpose |
| --- | --- |
| `.map(fn)` | Transform each row |
| `.filter(fn)` | Keep matching rows |
| `.clean(options)` | Trim, normalize whitespace, fuzzy-match headers, dedupe |
| `.schema(definition)` | Validate and coerce rows |
| `.take(n)` | Limit output |
| `.collect()` | Return rows |
| `.drain()` | Execute and return stats |
| `.process()` | Return rows, issues, and stats |
| `.write(dest, options)` | Write CSV, XLSX, or JSON |

---

## Schema Validation

```java
SchemaDefinition schema = new SchemaDefinition()
    .field("id", Schema.string())
    .field("email", Schema.email())
    .field("age", Schema.number(true))
    .field("active", Schema.bool().defaultValue(true))
    .field("signupDate", Schema.date());
```

Validation modes:

| Mode | Behavior |
| --- | --- |
| `FAIL_FAST` | Throw on the first invalid row |
| `COLLECT` | Keep valid rows and collect issues |
| `SKIP` | Drop invalid rows without collecting issues |

Field options include `optional`, `defaultValue`, `coerce`, and custom validators.

---

## Workbook Authoring

```java
Worksheet summary = new Worksheet("Summary",
    Arrays.asList(
        Row.of("metric", "Revenue", "value", 125000),
        Row.of("metric", "Target", "value", 100000),
        Row.of("metric", "Delta", "value", new FormulaCell("B2-B3", 25000))
    ));

summary.setFrozen(new FreezePane(0, 1, "A2"));
XlsxWriter.writeWorkbook(new Workbook(Collections.singletonList(summary)), "report.xlsx");
```

XLSX writing supports multiple sheets, formulas, frozen panes, column metadata, tables, and data validation helpers. Legacy `.xls` is read-only.

---

## Query, Diff, Join

```java
List<Row> top = Pravaah.query(rows,
    "SELECT id, name, revenue WHERE revenue >= 100000 ORDER BY revenue DESC LIMIT 25");

DiffResult changes = Pravaah.diff(beforeRows, afterRows, "customerId");

List<Row> enriched = Pravaah.joinRows(orders, customers, "customerId");
```

---

## Benchmarks

Every number below was measured locally with the current Java implementation on Apple Silicon with `-Xmx6g`. Each workload was warmed up once, then measured twice; the table reports the best observed time. Row counts are data rows, excluding the header row where applicable.

### Benchmark Notes

- Same dataset used across all libraries for each workload.
- JVM warmed before measurement.
- Best observed run reported after warmup.
- CSV competitors use their standard header-aware parsing APIs.
- XLS/XLSX competitors use Apache POI `WorkbookFactory` and EasyExcel sheet readers.
- Output was verified against competitors by row count and normalized row-value hashes.

### CSV Read/Count

CSV workloads are benchmarked against CSV competitors: uniVocity, Apache Commons CSV, OpenCSV, and Jackson CSV. Pravaah uses the direct count path, `CsvReader.drainCount(...)`, which parses CSV records without materializing `Row` objects.

```text
CSV: 7,046,063 rows, 145MB on JDK 17 - time (lower is better)
──────────────────────────────────────────────────────────────
Pravaah     ■■■■■■■                         330ms
uniVocity   ■■■■■■■■                        407ms
Commons CSV ■■■■■■■■■■■■■■■■■■■■          1.00s
Jackson CSV ■■■■■■■■■■■■■■■■■■■■■■■■      1.19s
OpenCSV     ■■■■■■■■■■■■■■■■■■■■■■■■■■■■  1.40s

CSV: 1,004,894 rows, 244MB on JDK 17 - time (lower is better)
──────────────────────────────────────────────────────────────
Pravaah     ■■■■■■■■■■                      348ms
uniVocity   ■■■■■■■■■■                      351ms
Jackson CSV ■■■■■■■■■■■■■■■■■■■■■■■       853ms
Commons CSV ■■■■■■■■■■■■■■■■■■■■■■■■      888ms
OpenCSV     ■■■■■■■■■■■■■■■■■■■■■■■■■■■   986ms
```

#### JDK 8

| Format | File size | Rows | Pravaah | uniVocity | Commons CSV | OpenCSV | Jackson CSV |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| CSV | 498 KB | 1,000 | 3 ms | 2 ms | 2 ms | 5 ms | 2 ms |
| CSV | 244 MB | 1,004,894 | 403 ms | 312 ms | 862 ms | 763 ms | 718 ms |
| CSV | 145 MB | 7,046,063 | 341 ms | 390 ms | 814 ms | 804 ms | 827 ms |

#### JDK 11

| Format | File size | Rows | Pravaah | uniVocity | Commons CSV | OpenCSV | Jackson CSV |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| CSV | 498 KB | 1,000 | 4 ms | 4 ms | 2 ms | 6 ms | 4 ms |
| CSV | 244 MB | 1,004,894 | 405 ms | 376 ms | 929 ms | 898 ms | 955 ms |
| CSV | 145 MB | 7,046,063 | 363 ms | 458 ms | 1.15 s | 1.27 s | 1.21 s |

#### JDK 17

| Format | File size | Rows | Pravaah | uniVocity | Commons CSV | OpenCSV | Jackson CSV |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| CSV | 498 KB | 1,000 | 4 ms | 2 ms | 2 ms | 5 ms | 3 ms |
| CSV | 244 MB | 1,004,894 | 348 ms | 351 ms | 888 ms | 986 ms | 853 ms |
| CSV | 145 MB | 7,046,063 | 330 ms | 407 ms | 1.00 s | 1.40 s | 1.19 s |

### Spreadsheet Read

Spreadsheet workloads are benchmarked against corresponding spreadsheet competitors: Apache POI and EasyExcel. Pravaah materializes rows through the XLS/XLSX readers.

```text
XLSX: 35,808 rows, 1.5MB on JDK 17 - time (lower is better)
──────────────────────────────────────────────────────────────
Pravaah   ■■■■■                           96ms
EasyExcel ■■■■■■■                         142ms
POI       ■■■■■■■■■■■■■■■■■■■■■■■■■■■■    513ms

XLS: 65,535 rows, 4.8MB on JDK 17 - time (lower is better)
──────────────────────────────────────────────────────────────
Pravaah   ■■■■■■                          43ms
EasyExcel ■■■■■■■■■■■■                    83ms
POI       ■■■■■■■■■■■■■■■■■■■■■■■■■■■■    183ms
```

#### JDK 8

| Format | File size | Rows | Pravaah | Apache POI | EasyExcel |
| --- | ---: | ---: | ---: | ---: | ---: |
| XLSX | 244 KB | 1,000 | 19 ms | 50 ms | 21 ms |
| XLSX | 1.5 MB | 35,808 | 96 ms | 408 ms | 133 ms |
| XLS | 4.8 MB | 65,535 | 52 ms | 160 ms | 67 ms |

#### JDK 11

| Format | File size | Rows | Pravaah | Apache POI | EasyExcel |
| --- | ---: | ---: | ---: | ---: | ---: |
| XLSX | 244 KB | 1,000 | 26 ms | 81 ms | 26 ms |
| XLSX | 1.5 MB | 35,808 | 120 ms | 554 ms | 147 ms |
| XLS | 4.8 MB | 65,535 | 46 ms | 210 ms | 90 ms |

#### JDK 17

| Format | File size | Rows | Pravaah | Apache POI | EasyExcel |
| --- | ---: | ---: | ---: | ---: | ---: |
| XLSX | 244 KB | 1,000 | 20 ms | 63 ms | 21 ms |
| XLSX | 1.5 MB | 35,808 | 96 ms | 513 ms | 142 ms |
| XLS | 4.8 MB | 65,535 | 43 ms | 183 ms | 83 ms |

### Format Coverage

The test suite verifies the same logical ingestion rows across CSV, XLS, and XLSX, including strings, numbers, booleans, blank cells, formulas with cached values, sheet selection, headerless reads, explicit headers, validation, and corrupt workbook handling.

Run your own benchmarks with the public API:

```java
long start = System.nanoTime();
List<Row> rows = Pravaah.read("large.csv").collect();
long ms = (System.nanoTime() - start) / 1_000_000;
System.out.println(rows.size() + " rows in " + ms + "ms");
```

No benchmark harness or downloaded competitor jars are required in the repository.

---

## How The Performance Works

**CSV:** A direct scanner walks the text once and emits fields to specialized sinks. Count-only scans avoid `Row` allocation. Validation can consume scanner output without first building a separate raw-row list.

**XLS:** The reader opens the OLE2 compound file, extracts the `Workbook` stream, and parses BIFF8 records directly. It handles shared strings, sheet metadata, numeric cells, RK cells, booleans, blanks, labels, and cached formula values.

**XLSX:** The reader targets workbook metadata and selected worksheet XML instead of building a full workbook object model.

**MR-JAR:** Java 8 uses the baseline runtime classes. Java 11 and Java 17+ load overlay implementations from `META-INF/versions/*`.

---

## API Reference

| API | Purpose |
| --- | --- |
| `Pravaah.read(source, options)` | Create a pipeline from CSV, XLS, XLSX, JSON, bytes, or rows |
| `Pravaah.write(rows, dest, options)` | Write CSV, XLSX, or JSON |
| `Pravaah.parse(source, schema, options)` | Validate and return typed rows |
| `Pravaah.parseDetailed(source, schema, options)` | Return rows, issues, and stats |
| `CsvReader.drainCount(source, options)` | Count CSV rows without materializing rows |
| `SchemaValidator.writeIssueReport(issues, dest)` | Write validation diagnostics as CSV |
| `Pravaah.query(rows, sql)` | SQL-like queries over rows |
| `Pravaah.diff(before, after, key)` | Dataset diffs by key |
| `Pravaah.joinRows(left, right, key)` | Join two row sets |
| `XlsxWriter.writeWorkbook(workbook, dest)` | Multi-sheet XLSX authoring |

### Read Options

| Option | Description |
| --- | --- |
| `format` | Force `CSV`, `XLS`, `XLSX`, or `JSON` |
| `sheetName` | Select a worksheet by name |
| `sheetIndex` | Select a worksheet by zero-based index |
| `headers` | Use first row as headers, or read headerless rows |
| `headerNames` | Supply explicit headers |
| `delimiter` | CSV delimiter, one character |
| `inferTypes` | Convert CSV strings to numbers, booleans, and nulls |
| `formulas` | Preserve formula metadata where supported |
| `validation` | `FAIL_FAST`, `COLLECT`, or `SKIP` |
| `cleaning` | Trim, normalize whitespace, fuzzy headers, dedupe |

---

## Development

```sh
mvn test
mvn package -DskipTests
```

The normal test suite has no competitor-library dependency.

---

## License

[MIT](LICENSE)
