package io.github.beingmartinbmc.pravaah;

import java.util.*;

public class ProcessStats {
    private int rowsProcessed;
    private int rowsWritten;
    private int errors;
    private int warnings;
    private long startedAt;
    private Long endedAt;
    private Long durationMs;
    private Long peakMemoryBytes;
    private final List<String> sheets = new ArrayList<>();

    public int getRowsProcessed() { return rowsProcessed; }
    public void setRowsProcessed(int v) { this.rowsProcessed = v; }
    public void incrementRowsProcessed() { this.rowsProcessed++; }

    public int getRowsWritten() { return rowsWritten; }
    public void setRowsWritten(int v) { this.rowsWritten = v; }
    public void incrementRowsWritten() { this.rowsWritten++; }

    public int getErrors() { return errors; }
    public void setErrors(int v) { this.errors = v; }
    public void addErrors(int n) { this.errors += n; }

    public int getWarnings() { return warnings; }
    public void setWarnings(int v) { this.warnings = v; }
    public void addWarnings(int n) { this.warnings += n; }

    public long getStartedAt() { return startedAt; }
    public void setStartedAt(long v) { this.startedAt = v; }

    public Long getEndedAt() { return endedAt; }
    public void setEndedAt(Long v) { this.endedAt = v; }

    public Long getDurationMs() { return durationMs; }
    public void setDurationMs(Long v) { this.durationMs = v; }

    public Long getPeakMemoryBytes() { return peakMemoryBytes; }
    public void setPeakMemoryBytes(Long v) { this.peakMemoryBytes = v; }

    public List<String> getSheets() { return sheets; }
}
