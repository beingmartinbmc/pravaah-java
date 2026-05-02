package io.github.beingmartinbmc.pravaah.internal.validation;

import io.github.beingmartinbmc.pravaah.CleaningOptions;
import io.github.beingmartinbmc.pravaah.PravaahIssue;
import io.github.beingmartinbmc.pravaah.PravaahValidationException;
import io.github.beingmartinbmc.pravaah.ProcessResult;
import io.github.beingmartinbmc.pravaah.ProcessStats;
import io.github.beingmartinbmc.pravaah.Row;
import io.github.beingmartinbmc.pravaah.ValidationMode;
import io.github.beingmartinbmc.pravaah.perf.PerfUtils;
import io.github.beingmartinbmc.pravaah.schema.SchemaDefinition;
import io.github.beingmartinbmc.pravaah.schema.SchemaValidator;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.IntConsumer;

public final class SchemaValidationRunner implements ValidationRunner {
    private final SchemaDefinition definition;
    private final ValidationMode mode;
    private final CleaningOptions cleaning;
    private final ProcessStats stats;
    private final List<Row> rows = new ArrayList<>();
    private final List<PravaahIssue> issues = new ArrayList<>();
    private final Set<String> seen = new HashSet<>();
    private final IntConsumer progressConsumer;
    private final Consumer<PravaahIssue> issueConsumer;
    private final boolean strictHeaders;
    private boolean headersChecked;

    public SchemaValidationRunner(SchemaDefinition definition, ValidationMode mode, CleaningOptions cleaning) {
        this(definition, mode, cleaning, null, null);
    }

    public SchemaValidationRunner(SchemaDefinition definition, ValidationMode mode, CleaningOptions cleaning,
                                  IntConsumer progressConsumer, Consumer<PravaahIssue> issueConsumer) {
        this(definition, mode, cleaning, progressConsumer, issueConsumer, false);
    }

    public SchemaValidationRunner(SchemaDefinition definition, ValidationMode mode, CleaningOptions cleaning,
                                  IntConsumer progressConsumer, Consumer<PravaahIssue> issueConsumer,
                                  boolean strictHeaders) {
        this.definition = definition;
        this.mode = mode;
        this.cleaning = cleaning;
        this.stats = PerfUtils.createStats();
        this.progressConsumer = progressConsumer;
        this.issueConsumer = issueConsumer;
        this.strictHeaders = strictHeaders;
    }

    @Override
    public void accept(Row row, int rowNumber) {
        if (strictHeaders && !headersChecked) {
            headersChecked = true;
            List<PravaahIssue> headerIssues = SchemaValidator.validateHeaders(row.keySet(), definition, cleaning);
            if (!headerIssues.isEmpty()) {
                issues.addAll(headerIssues);
                stats.addErrors(headerIssues.size());
                if (issueConsumer != null) {
                    for (PravaahIssue issue : headerIssues) issueConsumer.accept(issue);
                }
                if (mode == ValidationMode.FAIL_FAST) {
                    throw new PravaahValidationException(headerIssues);
                }
            }
        }

        Row cleaned = SchemaValidator.cleanRow(row, cleaning);
        if (cleaning != null && cleaning.isDropBlankRows() && SchemaValidator.isBlankRow(cleaned)) {
            return;
        }
        if (cleaning != null && SchemaValidator.isDuplicate(cleaned, cleaning.getDedupeKey(), seen)) {
            return;
        }

        SchemaValidator.ValidationResult result = SchemaValidator.validateRow(cleaned, definition, rowNumber);
        stats.incrementRowsProcessed();
        if (progressConsumer != null) progressConsumer.accept(stats.getRowsProcessed());
        if ((rowNumber & 4095) == 0) PerfUtils.observeMemory(stats);

        if (result.getValue() != null) {
            rows.add(result.getValue());
        } else if (mode != ValidationMode.SKIP) {
            issues.addAll(result.getIssues());
            stats.addErrors(result.getIssues().size());
            if (issueConsumer != null) {
                for (PravaahIssue issue : result.getIssues()) {
                    issueConsumer.accept(issue);
                }
            }
            if (mode == ValidationMode.FAIL_FAST) {
                throw new PravaahValidationException(result.getIssues());
            }
        }
    }

    @Override
    public ProcessResult finish() {
        return new ProcessResult(rows, issues, PerfUtils.finishStats(stats));
    }
}
