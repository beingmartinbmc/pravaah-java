# Pravaah Java

**Replace 300 lines of fragile CSV/Excel glue code with 10.**

Schema-first, streaming data pipeline library for **CSV, XLSX, and JSON** in Java 8+. Zero dependencies in production. Type-safe validation, cleaning, deduplication, and formula evaluation -- all in a fluent pipeline.

> *Pravaah* (Hindi: प्रवाह) means "flow" -- data flows through schemas, gets cleaned, validated, and lands where you need it.

```java
ProcessResult result = Pravaah.parseDetailed(
    csvBytes,
    new SchemaDefinition()
        .field("email",  Schema.email())
        .field("age",    Schema.number())
        .field("active", Schema.bool())
        .field("role",   Schema.string().defaultValue("user")),
    ReadOptions.defaults()
        .format(PravaahFormat.CSV)
        .validation(ValidationMode.COLLECT));

List<Row> validRows   = result.getRows();     // clean, typed rows
List<PravaahIssue> errors = result.getIssues();   // per-field errors with row numbers
```

That's it. **10 lines.** Email validated. Numbers coerced. Booleans normalized. Missing fields get defaults. Bad rows collected with actionable error reports.

The same task with `BufferedReader + String.split`? **120+ lines** -- and it still can't handle `"hello,world"`.

---

## Why Pravaah?

Most Java CSV libraries solve **parsing**. You still need to write validation, type coercion, cleaning, deduplication, error collection, and pipeline orchestration yourself.

| What you need | Commons CSV | OpenCSV | uniVocity | Jackson CSV | Pravaah |
|---|:---:|:---:|:---:|:---:|:---:|
| CSV parsing | Yes | Yes | Yes | Yes | Yes |
| XLSX read/write | No | No | No | No | **Yes** |
| JSON read/write | No | No | No | Partial | **Yes** |
| Schema validation | No | No | No | No | **Yes** |
| Email/phone validation | No | No | No | No | **Yes** |
| Type coercion | No | No | No | No | **Yes** |
| Fuzzy header matching | No | No | No | No | **Yes** |
| Deduplication | No | No | No | No | **Yes** |
| Error collection | No | No | No | No | **Yes** |
| Formula engine | No | No | No | No | **Yes** |
| SQL-like queries | No | No | No | No | **Yes** |
| Dataset diffing | No | No | No | No | **Yes** |
| Plugin system | No | No | No | No | **Yes** |
| Zero dependencies | No | No | Yes | No | **Yes** |
| Java 8 compatible | Yes | Yes | Yes | Yes | **Yes** |

---

## Benchmarks

**Machine:** Apple Silicon M3 Pro, 16 cores, 36 GB RAM, Zulu OpenJDK 1.8.0_491 (aarch64), `-Xmx6g`

### 1. CSV Parsing Speed

Raw parse performance. Pravaah handles **RFC-compliant quoted fields, multi-line values, and CRLF** -- the DIY approach doesn't.

| Rows | Pravaah | BufferedReader + split | String.split |
|---:|---:|---:|---:|
| 10K | 1 ms | <1 ms | <1 ms |
| 100K | 11 ms | 10 ms | 9 ms |
| **1M** | **145 ms** | 105 ms | 116 ms |

> At 100K rows, Pravaah is **within 1 ms** of raw `split()`. At 1M rows it's ~1.4x -- **but `split()` breaks on the first quoted comma.** Pravaah handles every RFC 4180 edge case correctly.

### 2. Validation Pipeline

Parse + validate email + coerce types + collect errors. **100K rows, ~10% bad data.**

| Approach | Time | Valid Rows | Errors | LOC |
|---|---:|---:|---:|---:|
| **Pravaah** | **42 ms** | 90,027 | 29,919 | **10** |
| DIY (BufferedReader + regex + try/catch) | 21 ms | 90,027 | 29,919 | **120+** |

> DIY is faster because it does less -- no Row objects, no schema introspection, no issue objects with row numbers and field names. But you write and maintain 120 lines for every new file format. Pravaah gives you a **12x reduction in code** for a 2x time cost that's invisible at application scale.

### 3. Large File Streaming

Processing millions of rows with filtering and full schema validation.

| Rows | Operation | Time | Throughput | Peak Memory |
|---:|---|---:|---:|---:|
| 1M | read + filter + drain | 263 ms | 3.8M rows/sec | 743 MB |
| 1M | full schema validation | 523 ms | 1.9M rows/sec | -- |
| 2M | read + filter + drain | 546 ms | 3.7M rows/sec | 1.4 GB |
| 2M | full schema validation | 1,164 ms | 1.7M rows/sec | -- |
| **5M** | read + filter + drain | **6,044 ms** | **827K rows/sec** | 3.5 GB |

> **3.8 million rows per second** at 1M rows with filtering. 1.9M rows/sec with full email + number + boolean + string validation on every field. At 5M rows the JVM's GC pressure under Java 8 is visible -- Java 11+ with G1GC handles this range significantly better.

### 4. The Real Benchmark: Lines of Code

```
┌────────────────────────────────┬───────┐
│ Approach                       │  LOC  │
├────────────────────────────────┼───────┤
│ Pravaah                        │    10 │
│ uniVocity + DIY validation     │   60+ │
│ OpenCSV + DIY validation       │   70+ │
│ Apache Commons CSV + DIY valid │   85+ │
│ Jackson CSV + DIY validation   │   90+ │
│ BufferedReader + split + DIY   │  120+ │
│ Full DIY (quoted CSV + valid)  │  300+ │
└────────────────────────────────┴───────┘
```

Every other library gives you a `List<String[]>`. You still need to:
- Map columns to field names
- Validate email format
- Parse numbers (and handle `NumberFormatException`)
- Coerce booleans (`"yes"`, `"1"`, `"true"`, `"Y"` -> `true`)
- Handle missing fields with defaults
- Collect errors with row numbers
- Handle duplicates
- Normalize headers (`"Email Address"` -> `"email"`)

**Pravaah does all of this declaratively.**

---

## Quick Start

### Maven

```xml
<dependency>
    <groupId>io.github.beingmartinbmc</groupId>
    <artifactId>pravaah-java</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

### Read CSV

```java
List<Row> rows = Pravaah.read("data.csv").collect();
```

### Read with Schema Validation

```java
ProcessResult result = Pravaah.parseDetailed(
    "leads.csv",
    new SchemaDefinition()
        .field("email", Schema.email())
        .field("name",  Schema.string())
        .field("score", Schema.number())
        .field("active", Schema.bool()),
    ReadOptions.defaults()
        .format(PravaahFormat.CSV)
        .validation(ValidationMode.COLLECT));

System.out.println(result.getRows().size() + " valid rows");
System.out.println(result.getIssues().size() + " validation errors");
```

### Pipeline with Cleaning + Validation

```java
List<Row> clean = Pravaah.read("messy.csv")
    .clean(CleaningOptions.defaults()
        .trim(true)
        .normalizeWhitespace(true)
        .dedupeKey("id", "email")
        .fuzzyHeader("email", "E-mail", "Email Address", "email_addr"))
    .schema(new SchemaDefinition()
        .field("email", Schema.email())
        .field("age",   Schema.number())
        .field("role",  Schema.string().defaultValue("user")))
    .filter(row -> ((Number) row.get("age")).doubleValue() >= 18)
    .collect();
```

### Read & Write XLSX

```java
// Read
List<Row> finance = Pravaah.read("report.xlsx",
    ReadOptions.defaults().sheetName("Q4")).collect();

// Write multi-sheet workbook
Workbook wb = new Workbook(Arrays.asList(
    new Worksheet("Leads", leadRows),
    new Worksheet("Revenue", revenueRows)));
XlsxWriter.writeWorkbook(wb, "output.xlsx");
```

### JSON

```java
List<Row> rows = Pravaah.read("data.json",
    ReadOptions.defaults().format(PravaahFormat.JSON)).collect();

Pravaah.write(rows, "output.json",
    WriteOptions.defaults().format(PravaahFormat.JSON));
```

### Formula Engine

```java
Object result = FormulaEngine.evaluateFormula(
    "=SUM(price, tax)", Row.of("price", 100, "tax", 8.5));
// -> 108.5

// Custom formulas
FormulaEngine engine = new FormulaEngine();
engine.register("DISCOUNT", (args, row) ->
    ((Number) args.get(0)).doubleValue() * 0.9);
engine.evaluate("DISCOUNT(total)", Row.of("total", 100));
// -> 90.0
```

### SQL-like Queries

```java
List<Row> top = Pravaah.query(rows,
    "SELECT name, score WHERE score >= 80 ORDER BY score DESC LIMIT 10");
```

### Dataset Diff

```java
DiffEngine.DiffResult diff = Pravaah.diff(lastMonth, thisMonth, "id");
System.out.println(diff.getAdded().size() + " new records");
System.out.println(diff.getRemoved().size() + " deleted");
System.out.println(diff.getChanged().size() + " modified");

DiffEngine.writeDiffReport(diff, "changes.csv");
```

### Plugin System

```java
PluginRegistry registry = new PluginRegistry();
registry.use(new PravaahPlugin("crm")
    .formulas(Map.of("LEAD_SCORE", (args, row) -> calculateScore(args)))
    .validators(List.of(row -> {
        if (row.get("company") == null)
            return List.of(PravaahIssue.error("missing_company", "Company required", ...));
        return Collections.emptyList();
    })));
```

---

## Modules

| Module | Description |
|---|---|
| `csv` | RFC 4180 compliant streaming CSV parser and writer. Handles quoted fields, multi-line values, CRLF, custom delimiters. |
| `xlsx` | Zero-dependency XLSX reader and writer using ZIP + XML. Multi-sheet, formulas, frozen panes, data validation. |
| `schema` | Declarative field definitions: STRING, NUMBER, BOOLEAN, DATE, EMAIL, PHONE, ANY. Coercion, defaults, custom validators. |
| `formula` | Expression engine with SUM, AVERAGE, MIN, MAX, COUNT, IF, CONCAT. Arithmetic parser. Extensible. |
| `query` | SQL-like SELECT/WHERE/ORDER BY/LIMIT over row datasets. Supports =, !=, >, >=, <, <=, contains. |
| `diff` | Key-based dataset comparison. Reports added, removed, changed rows with per-column change tracking. |
| `pipeline` | Lazy evaluation pipeline: map, filter, clean, schema, take. Terminal ops: collect, drain, process, write. |
| `plugin` | Extensible registry for custom validators and formula functions. |
| `perf` | Processing stats: duration, row counts, peak memory, timing. |

---

## Schema Types

| Type | Coerces From | Validates |
|---|---|---|
| `Schema.string()` | Any value via `toString()` | Always valid |
| `Schema.number()` | `"42"` -> `42.0` | Finite numbers only |
| `Schema.bool()` | `"yes"`, `"1"`, `"true"`, `"y"` -> `true`; `"no"`, `"0"`, `"false"`, `"n"` -> `false` | Recognized boolean strings |
| `Schema.date()` | ISO-8601 strings (`"2024-01-15"`, `"2024-01-15T10:30:00Z"`) | `LocalDate`, `Instant`, `LocalDateTime` |
| `Schema.email()` | Trims whitespace | `user@domain.tld` format |
| `Schema.phone()` | Strips non-digit characters | Minimum 7 digits |
| `Schema.any()` | Passthrough | Always valid |

All types support `.optional(true)`, `.defaultValue(x)`, `.coerce(false)`, and `.validate((value, row) -> errorOrNull)`.

---

## Validation Modes

| Mode | Behavior |
|---|---|
| `COLLECT` | Validate all rows, collect all errors, return valid rows + error list |
| `FAIL_FAST` | Stop on first error, throw `PravaahValidationException` |
| `SKIP` | Silently drop invalid rows, no error tracking |

---

## Running Benchmarks

```bash
mvn test-compile
java -Xmx6g -cp target/classes:target/test-classes \
  io.github.beingmartinbmc.pravaah.BenchmarkRunner
```

The benchmark generates CSV files (10K to 5M rows), runs each test 5 times, and reports median timings.

---

## Running Tests

```bash
mvn test
```

**205 tests** covering all modules with 90%+ code coverage.

---

## Java 8 Compatibility

Pravaah targets Java 8 (`-source 1.8 -target 1.8`). No `var`, no records, no text blocks, no `List.of()`. Works on Java 8 through 21+.

---

## License

[MIT](LICENSE)
