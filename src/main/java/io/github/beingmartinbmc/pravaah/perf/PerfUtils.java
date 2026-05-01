package io.github.beingmartinbmc.pravaah.perf;

import io.github.beingmartinbmc.pravaah.ProcessStats;

public final class PerfUtils {

    private PerfUtils() {}

    public static ProcessStats createStats() {
        ProcessStats stats = new ProcessStats();
        stats.setStartedAt(System.currentTimeMillis());
        stats.setPeakMemoryBytes(Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory());
        return stats;
    }

    public static void observeMemory(ProcessStats stats) {
        long used = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        if (stats.getPeakMemoryBytes() == null || used > stats.getPeakMemoryBytes()) {
            stats.setPeakMemoryBytes(used);
        }
    }

    public static ProcessStats finishStats(ProcessStats stats) {
        observeMemory(stats);
        long endedAt = System.currentTimeMillis();
        stats.setEndedAt(endedAt);
        stats.setDurationMs(endedAt - stats.getStartedAt());
        return stats;
    }

    public static ProcessStats mergeStats(ProcessStats target, ProcessStats source) {
        target.setRowsProcessed(target.getRowsProcessed() + source.getRowsProcessed());
        target.setRowsWritten(target.getRowsWritten() + source.getRowsWritten());
        target.setErrors(target.getErrors() + source.getErrors());
        target.setWarnings(target.getWarnings() + source.getWarnings());
        long tp = target.getPeakMemoryBytes() != null ? target.getPeakMemoryBytes() : 0;
        long sp = source.getPeakMemoryBytes() != null ? source.getPeakMemoryBytes() : 0;
        target.setPeakMemoryBytes(Math.max(tp, sp));
        return target;
    }

    public static String formatBytes(long bytes) {
        if (bytes < 1024) return bytes + "B";
        if (bytes < 1024 * 1024) return String.format("%.1fKB", bytes / 1024.0);
        return String.format("%.1fMB", bytes / (1024.0 * 1024.0));
    }
}
