package io.github.beingmartinbmc.pravaah;

public enum PravaahFormat {
    CSV,
    XLS,
    XLSX,
    JSON;

    public static PravaahFormat fromExtension(String path) {
        if (path == null) return XLSX;
        String lower = path.toLowerCase();
        if (lower.endsWith(".csv")) return CSV;
        if (lower.endsWith(".json")) return JSON;
        if (lower.endsWith(".xls")) return XLS;
        if (lower.endsWith(".xlsx")) return XLSX;
        return XLSX;
    }
}
