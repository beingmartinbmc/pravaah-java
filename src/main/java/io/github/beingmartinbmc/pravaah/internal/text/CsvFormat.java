package io.github.beingmartinbmc.pravaah.internal.text;

/**
 * Tiny CSV formatting helpers shared by report writers. The library's hot
 * write path lives in {@link io.github.beingmartinbmc.pravaah.csv.CsvWriter}
 * and intentionally uses a hand-rolled byte-level encoder; this class is only
 * used by lower-volume report writers (diff reports, validation issue dumps).
 */
public final class CsvFormat {

    private CsvFormat() {}

    /**
     * Quote-and-escape {@code value} per RFC 4180 if it contains a delimiter, a
     * double quote or a newline. Returns the input unchanged otherwise.
     */
    public static String csvEscape(String value) {
        if (value.indexOf(',') < 0 && value.indexOf('"') < 0 && value.indexOf('\n') < 0) {
            return value;
        }
        return "\"" + value.replace("\"", "\"\"") + "\"";
    }
}
