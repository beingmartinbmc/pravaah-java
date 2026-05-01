# Pravaah Java

Schema-first data ingestion for Java 8+: CSV, XLS, XLSX, and JSON.

Pravaah turns customer uploads into trusted application rows: read files, normalize messy headers, validate schemas, collect rejected rows, and write clean output without parser glue code.

Java 8 compatible. Zero production dependencies. Multi-release JAR optimized for Java 8, 11, and 17+ runtimes.

```text
CSV direct count: 7,046,063 rows, 145MB
Pravaah Java  450ms

CSV collect: 1,004,894 rows, 244MB
Pravaah Java  1.06s
```

Benchmarked locally on the same files used by the Sheetra/Node README, using JDK 17 on Apple Silicon.

---

## 30-Second Win

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

## Core Capabilities

- CSV parser with quoted fields, escaped quotes, embedded newlines, CRLF, custom delimiters, and direct count scans.
- XLS read support through an internal OLE2/BIFF8 parser.
- XLSX read/write support through ZIP/XML parsing and writer utilities.
- JSON read/write for fixtures and pipeline output.
- Schema validation for string, number, boolean, date, email, phone, and any values.
- Cleaning with trim, whitespace normalization, fuzzy header aliases, and deduplication.
- Issue reports with row numbers, column names, expected types, and raw values.
- Lazy pipeline API with map, filter, clean, schema, take, collect, drain, process, and write.
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

Every number below was measured locally with the current Java implementation, JDK 17, Apple Silicon, best observed run after warmup. The CSV files are the same benchmark files used by the Sheetra README.

### CSV Read

| Workload | Size | Rows | Direct count | Collect rows |
| --- | ---: | ---: | ---: | ---: |
| `MOCK_DATA.csv` | 498 KB | 1,000 | 1 ms | 1 ms |
| `Crime_Data_from_2020_to_2024.csv` | 244 MB | 1,004,894 | 417 ms | 1.06 s |
| `geographic-units...2025.csv` | 145 MB | 7,046,063 | 450 ms | 1.30 s |

Direct count uses `CsvReader.drainCount(...)`. Collect rows uses `Pravaah.read(...).collect()`.

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
