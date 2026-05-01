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

**Machine:** Apple Silicon M3 Pro, 16 cores, 36 GB RAM, `-Xmx6g`

Benchmarked against **all major Java CSV libraries** across **4 JDK versions**:

| JDK | Version | GC |
|---|---|---|
| JDK 8 | Zulu OpenJDK 1.8.0_491 | Parallel GC |
| JDK 11 | JBR-DCEVM 11.0.16 | G1GC |
| JDK 17 | Corretto 17.0.18 | G1GC |
| JDK 25 | Zulu 25.0.1.0.101 (OneJDK) | G1GC |

### 1. CSV Parsing Speed — 1M Rows (43.6 MB)

Raw parse performance. All libraries parse the same 1M-row CSV. Median of 5 runs after 3 warmup iterations.

| Library | JDK 8 | JDK 11 | JDK 17 | JDK 25 |
|---|---:|---:|---:|---:|
| **Pravaah** | **99 ms** | 353 ms | 199 ms | 193 ms |
| BufferedReader + split | 127 ms | 123 ms | 96 ms | 114 ms |
| Apache Commons CSV | 157 ms | 233 ms | 221 ms | 236 ms |
| **uniVocity-parsers** | **65 ms** | 152 ms | 139 ms | 141 ms |
| OpenCSV | 127 ms | 183 ms | 163 ms | 155 ms |
| Jackson CSV | 137 ms | 146 ms | 131 ms | 156 ms |

**Scaling across sizes (JDK 17):**

| Rows | Pravaah | uniVocity | Jackson CSV | OpenCSV | Commons CSV | BR+split |
|---:|---:|---:|---:|---:|---:|---:|
| 10K | 1 ms | 2 ms | 2 ms | 2 ms | 2 ms | 1 ms |
| 100K | 22 ms | 11 ms | 13 ms | 14 ms | 21 ms | 9 ms |
| 1M | 199 ms | 139 ms | 131 ms | 163 ms | 221 ms | 96 ms |

> **Key insight:** `BufferedReader + split` is the fastest raw parser, but it **breaks on the first quoted comma**. Pravaah handles every RFC 4180 edge case (quoted fields, multi-line values, embedded commas, CRLF). uniVocity is the fastest feature-complete parser for raw parsing. Pravaah is competitive and brings schema validation that none of the others include.

### 2. Large File Streaming — 1M Rows, Throughput & Memory

| Library | JDK 8 | JDK 11 | JDK 17 | JDK 25 |
|---|---:|---:|---:|---:|
| **Pravaah** | 102 ms / 1.08 GB | 359 ms / 690 MB | 228 ms / 702 MB | 195 ms / 691 MB |
| BufferedReader+split | 129 ms / 888 MB | 138 ms / 231 MB | 96 ms / 116 MB | 114 ms / 34 MB |
| Apache Commons CSV | 158 ms / 362 MB | 235 ms / 47 MB | 220 ms / 80 MB | 235 ms / 94 MB |
| uniVocity-parsers | 65 ms / 430 MB | 156 ms / 362 MB | 144 ms / 383 MB | 142 ms / 386 MB |
| OpenCSV | 126 ms / 592 MB | 183 ms / 41 MB | 169 ms / 36 MB | 163 ms / 63 MB |
| Jackson CSV | 136 ms / 625 MB | 136 ms / 16 MB | 132 ms / 136 MB | 162 ms / 90 MB |
| Pravaah (full schema) | 1,937 ms | 702 ms | 597 ms | 523 ms |

*Format: time / peak memory. Full schema = parse + email/number/bool/string validation on every field.*

> **Pravaah's full pipeline (parse + schema + validate)** runs at **1.9M rows/sec on JDK 25** — no other library in this table includes validation at all. The full-schema pass on JDK 25 is **3.7x faster** than on JDK 8 thanks to improved GC and JIT.

### 3. Validation Pipeline — 100K Rows, ~10% Bad Data

Parse + validate email format + coerce number/boolean + collect errors with row-level diagnostics.

| Approach | JDK 8 | JDK 11 | JDK 17 | JDK 25 | LOC |
|---|---:|---:|---:|---:|---:|
| **Pravaah** | 45 ms | 73 ms | 55 ms | 67 ms | **10** |
| DIY (BR + regex + try/catch) | 22 ms | 29 ms | 22 ms | 26 ms | **120+** |

> DIY is faster because it does less — no Row objects, no schema introspection, no issue objects with row numbers and field names. But you write and maintain 120 lines for every file format. **None of the competitor libraries include validation** — you'd write the same 120+ lines of DIY code on top of any of them.

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

### 5. JDK Version Insights

| Observation | Details |
|---|---|
| **JDK 8 has lowest parse latency** | Parallel GC with 6 GB heap runs aggressively; good for batch parsing but 1+ GB peak memory |
| **JDK 11+ dramatically reduces memory** | G1GC keeps peak memory 2-10x lower (Jackson CSV: 625 MB on JDK 8 → 16 MB on JDK 11) |
| **JDK 17/25 best for Pravaah full pipeline** | Full schema validation: 1,937 ms (JDK 8) → 523 ms (JDK 25) — **3.7x faster** |
| **uniVocity fastest raw parser** | Consistently #1 for raw CSV parsing across all JDKs |
| **Pravaah's value is in the pipeline** | 2-3x slower on raw parsing, but the only library with built-in validation, coercion, and error collection |

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
# Download competitor JARs (one-time)
mkdir -p benchmark-libs
curl -sL https://repo1.maven.org/maven2/com/univocity/univocity-parsers/2.9.1/univocity-parsers-2.9.1.jar -o benchmark-libs/univocity-parsers-2.9.1.jar
curl -sL https://repo1.maven.org/maven2/org/apache/commons/commons-csv/1.12.0/commons-csv-1.12.0.jar -o benchmark-libs/commons-csv-1.12.0.jar
curl -sL https://repo1.maven.org/maven2/com/opencsv/opencsv/5.9/opencsv-5.9.jar -o benchmark-libs/opencsv-5.9.jar
curl -sL https://repo1.maven.org/maven2/com/fasterxml/jackson/dataformat/jackson-dataformat-csv/2.18.4/jackson-dataformat-csv-2.18.4.jar -o benchmark-libs/jackson-dataformat-csv-2.18.4.jar
# (also download transitive deps: jackson-core, jackson-databind, jackson-annotations,
#  commons-io, commons-codec, commons-lang3, commons-text, commons-collections4, commons-beanutils, commons-logging)

# Compile and run
mvn compile test-compile
java -Xmx6g -cp target/classes:target/test-classes:benchmark-libs/* \
  io.github.beingmartinbmc.pravaah.BenchmarkRunner

# Run across all JDK versions
chmod +x run-benchmark.sh && ./run-benchmark.sh
```

The benchmark generates CSV files (10K to 1M rows), runs each test 5 times after 3 warmup iterations, and reports median timings. Results are saved to `benchmark-results/`.

---

## Running Tests

```bash
mvn test
```

**205 tests** covering all modules with 90%+ code coverage.

---

## Java 8 Compatibility

Pravaah targets Java 8 (`-source 1.8 -target 1.8`). No `var`, no records, no text blocks, no `List.of()`. Tested and benchmarked on Java 8, 11, 17, and 25.

---

## License

[MIT](LICENSE)
