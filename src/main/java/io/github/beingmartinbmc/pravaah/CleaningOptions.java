package io.github.beingmartinbmc.pravaah;

import java.util.*;

public class CleaningOptions {
    private boolean trim;
    private boolean normalizeWhitespace;
    private boolean dropBlankRows;
    private List<String> dedupeKey;
    private Map<String, List<String>> fuzzyHeaders = new LinkedHashMap<>();

    public boolean isTrim() { return trim; }
    public CleaningOptions trim(boolean t) { this.trim = t; return this; }

    public boolean isNormalizeWhitespace() { return normalizeWhitespace; }
    public CleaningOptions normalizeWhitespace(boolean n) { this.normalizeWhitespace = n; return this; }

    public boolean isDropBlankRows() { return dropBlankRows; }
    public CleaningOptions dropBlankRows(boolean d) { this.dropBlankRows = d; return this; }

    public List<String> getDedupeKey() { return dedupeKey; }
    public CleaningOptions dedupeKey(String... keys) {
        this.dedupeKey = Arrays.asList(keys);
        return this;
    }

    public Map<String, List<String>> getFuzzyHeaders() { return fuzzyHeaders; }
    public CleaningOptions fuzzyHeaders(Map<String, List<String>> f) { this.fuzzyHeaders = f; return this; }
    public CleaningOptions fuzzyHeader(String canonical, String... aliases) {
        this.fuzzyHeaders.put(canonical, Arrays.asList(aliases));
        return this;
    }

    public static CleaningOptions defaults() { return new CleaningOptions(); }
}
