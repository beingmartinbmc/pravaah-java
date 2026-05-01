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

public final class SchemaValidationRunner implements ValidationRunner {
    private final SchemaDefinition definition;
    private final ValidationMode mode;
    private final CleaningOptions cleaning;
    private final ProcessStats stats;
    private final List<Row> rows = new ArrayList<>();
    private final List<PravaahIssue> issues = new ArrayList<>();
    private final Set<String> seen = new HashSet<>();

    public SchemaValidationRunner(SchemaDefinition definition, ValidationMode mode, CleaningOptions cleaning) {
        this.definition = definition;
        this.mode = mode;
        this.cleaning = cleaning;
        this.stats = PerfUtils.createStats();
    }

    @Override
    public void accept(Row row, int rowNumber) {
        Row cleaned = SchemaValidator.cleanRow(row, cleaning);
        if (cleaning != null && SchemaValidator.isDuplicate(cleaned, cleaning.getDedupeKey(), seen)) {
            return;
        }

        SchemaValidator.ValidationResult result = SchemaValidator.validateRow(cleaned, definition, rowNumber);
        stats.incrementRowsProcessed();
        if ((rowNumber & 4095) == 0) PerfUtils.observeMemory(stats);

        if (result.getValue() != null) {
            rows.add(result.getValue());
        } else if (mode != ValidationMode.SKIP) {
            issues.addAll(result.getIssues());
            stats.addErrors(result.getIssues().size());
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
