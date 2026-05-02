package io.github.beingmartinbmc.pravaah.pipeline;

import io.github.beingmartinbmc.pravaah.*;
import io.github.beingmartinbmc.pravaah.csv.CsvWriter;
import io.github.beingmartinbmc.pravaah.internal.json.JsonRowWriter;
import io.github.beingmartinbmc.pravaah.perf.PerfUtils;
import io.github.beingmartinbmc.pravaah.schema.*;
import io.github.beingmartinbmc.pravaah.xlsx.XlsxWriter;

import java.io.IOException;
import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * A lazy pipeline that chains map, filter, clean, schema, and take operations.
 * Nothing executes until a terminal operation is called: collect(), drain(), process(), or write().
 */
public class PravaahPipeline {

    /** Sample memory usage every {@value} rows during map/filter execution. */
    private static final int MEMORY_OBSERVATION_INTERVAL = 4096;

    @FunctionalInterface
    public interface RowSupplier {
        List<Row> get() throws IOException;
    }

    private final RowSupplier source;
    private final List<PipelineOp> ops = new ArrayList<>();

    public PravaahPipeline(RowSupplier source) {
        this.source = source;
    }

    public PravaahPipeline map(Function<Row, Row> mapper) {
        PravaahPipeline next = copy();
        next.ops.add(new MapOp(mapper));
        return next;
    }

    public PravaahPipeline filter(Predicate<Row> predicate) {
        PravaahPipeline next = copy();
        next.ops.add(new FilterOp(predicate));
        return next;
    }

    public PravaahPipeline clean(CleaningOptions options) {
        PravaahPipeline next = copy();
        next.ops.add(new CleanOp(options));
        return next;
    }

    public PravaahPipeline schema(SchemaDefinition definition) {
        return schema(definition, null, null);
    }

    public PravaahPipeline schema(SchemaDefinition definition, ValidationMode validation, CleaningOptions cleaning) {
        PravaahPipeline next = copy();
        next.ops.add(new SchemaOp(definition, validation, cleaning));
        return next;
    }

    public PravaahPipeline take(int limit) {
        PravaahPipeline next = copy();
        next.ops.add(new TakeOp(limit));
        return next;
    }

    public List<Row> collect() throws IOException {
        return execute().rows;
    }

    public ProcessStats drain() throws IOException {
        ExecuteResult result = execute();
        return result.stats;
    }

    public ProcessResult process() throws IOException {
        ExecuteResult result = execute();
        return new ProcessResult(result.rows, result.issues, result.stats);
    }

    public ProcessStats write(String destination, WriteOptions options) throws IOException {
        List<Row> rows = collect();
        if (options.getSchema() != null) {
            ValidationMode mode = options.getValidation() != null ? options.getValidation() : ValidationMode.FAIL_FAST;
            ProcessResult result = SchemaValidator.validateRows(rows, options.getSchema(), mode, options.getCleaning());
            rows = result.getRows();
        }
        ProcessStats stats = PerfUtils.createStats();
        stats.setRowsProcessed(rows.size());
        stats.setRowsWritten(rows.size());

        PravaahFormat format = options.getFormat();
        if (format == null) format = PravaahFormat.fromExtension(destination);

        switch (format) {
            case CSV:
                CsvWriter.write(rows, destination, options);
                break;
            case XLSX:
                XlsxWriter.writeRows(rows, destination, options);
                break;
            case JSON:
                writeJson(rows, destination);
                break;
            default:
                throw new IllegalArgumentException("Unsupported write format: " + format);
        }

        return PerfUtils.finishStats(stats);
    }

    private ExecuteResult execute() throws IOException {
        ProcessStats stats = PerfUtils.createStats();
        List<Row> rows = source.get();
        List<PravaahIssue> issues = new ArrayList<>();
        int memoryCounter = 0;

        for (PipelineOp op : ops) {
            List<Row> nextRows = new ArrayList<>();

            if (op instanceof SchemaOp) {
                SchemaOp schemaOp = (SchemaOp) op;
                int rowNumber = 1;
                Set<String> seen = new HashSet<>();
                for (Row row : rows) {
                    Row cleaned = SchemaValidator.cleanRow(row, schemaOp.cleaning);
                    if (schemaOp.cleaning != null && schemaOp.cleaning.isDropBlankRows()
                            && SchemaValidator.isBlankRow(cleaned)) {
                        rowNumber++;
                        continue;
                    }
                    if (schemaOp.cleaning != null && SchemaValidator.isDuplicate(cleaned,
                            schemaOp.cleaning.getDedupeKey(), seen)) {
                        rowNumber++;
                        continue;
                    }
                    SchemaValidator.ValidationResult result = SchemaValidator.validateRow(cleaned, schemaOp.definition, rowNumber);
                    if (result.getValue() != null) {
                        nextRows.add(result.getValue());
                    } else {
                        ValidationMode mode = schemaOp.validation;
                        if (mode == ValidationMode.FAIL_FAST) {
                            issues.addAll(result.getIssues());
                            stats.addErrors(result.getIssues().size());
                            break;
                        } else if (mode != ValidationMode.SKIP) {
                            issues.addAll(result.getIssues());
                            stats.addErrors(result.getIssues().size());
                        }
                    }
                    rowNumber++;
                }
                rows = nextRows;
                continue;
            }

            if (op instanceof CleanOp) {
                CleanOp cleanOp = (CleanOp) op;
                Set<String> seen = new HashSet<>();
                for (Row row : rows) {
                    Row cleaned = SchemaValidator.cleanRow(row, cleanOp.options);
                    if (cleanOp.options.isDropBlankRows() && SchemaValidator.isBlankRow(cleaned)) {
                        continue;
                    }
                    if (cleanOp.options.getDedupeKey() != null
                            && SchemaValidator.isDuplicate(cleaned, cleanOp.options.getDedupeKey(), seen)) {
                        continue;
                    }
                    nextRows.add(cleaned);
                }
                rows = nextRows;
                continue;
            }

            if (op instanceof TakeOp) {
                int limit = ((TakeOp) op).limit;
                rows = rows.size() > limit ? rows.subList(0, limit) : rows;
                continue;
            }

            for (Row row : rows) {
                if (op instanceof MapOp) {
                    nextRows.add(((MapOp) op).mapper.apply(row));
                } else if (op instanceof FilterOp) {
                    if (((FilterOp) op).predicate.test(row)) {
                        nextRows.add(row);
                    }
                }
                memoryCounter++;
                if (memoryCounter % MEMORY_OBSERVATION_INTERVAL == 0) PerfUtils.observeMemory(stats);
            }
            rows = nextRows;
        }

        stats.setRowsProcessed(rows.size());
        PerfUtils.finishStats(stats);
        return new ExecuteResult(rows, issues, stats);
    }

    private PravaahPipeline copy() {
        PravaahPipeline next = new PravaahPipeline(this.source);
        next.ops.addAll(this.ops);
        return next;
    }

    private void writeJson(List<Row> rows, String destination) throws IOException {
        JsonRowWriter.writeRowsToFile(rows, destination);
    }

    private static class ExecuteResult {
        final List<Row> rows;
        final List<PravaahIssue> issues;
        final ProcessStats stats;

        ExecuteResult(List<Row> rows, List<PravaahIssue> issues, ProcessStats stats) {
            this.rows = rows;
            this.issues = issues;
            this.stats = stats;
        }
    }

    private interface PipelineOp {}

    private static class MapOp implements PipelineOp {
        final Function<Row, Row> mapper;
        MapOp(Function<Row, Row> mapper) { this.mapper = mapper; }
    }

    private static class FilterOp implements PipelineOp {
        final Predicate<Row> predicate;
        FilterOp(Predicate<Row> predicate) { this.predicate = predicate; }
    }

    private static class CleanOp implements PipelineOp {
        final CleaningOptions options;
        CleanOp(CleaningOptions options) { this.options = options; }
    }

    private static class SchemaOp implements PipelineOp {
        final SchemaDefinition definition;
        final ValidationMode validation;
        final CleaningOptions cleaning;
        SchemaOp(SchemaDefinition definition, ValidationMode validation, CleaningOptions cleaning) {
            this.definition = definition;
            this.validation = validation;
            this.cleaning = cleaning;
        }
    }

    private static class TakeOp implements PipelineOp {
        final int limit;
        TakeOp(int limit) { this.limit = limit; }
    }
}
