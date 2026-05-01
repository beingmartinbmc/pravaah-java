package io.github.beingmartinbmc.pravaah;

import java.util.List;

public class ProcessResult {
    private final List<Row> rows;
    private final List<PravaahIssue> issues;
    private final ProcessStats stats;

    public ProcessResult(List<Row> rows, List<PravaahIssue> issues, ProcessStats stats) {
        this.rows = rows;
        this.issues = issues;
        this.stats = stats;
    }

    public List<Row> getRows() { return rows; }
    public List<PravaahIssue> getIssues() { return issues; }
    public ProcessStats getStats() { return stats; }
}
