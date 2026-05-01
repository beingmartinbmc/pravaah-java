package io.github.beingmartinbmc.pravaah;

public enum PravaahFormat {
    CSV,
    XLSX,
    JSON;

    public static PravaahFormat fromExtension(String path) {
        if (path == null) return XLSX;
        if (path.endsWith(".csv")) return CSV;
        if (path.endsWith(".json")) return JSON;
        if (path.endsWith(".xlsx")) return XLSX;
        return XLSX;
    }
}
