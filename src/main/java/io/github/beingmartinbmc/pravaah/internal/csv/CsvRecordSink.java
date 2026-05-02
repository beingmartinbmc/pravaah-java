package io.github.beingmartinbmc.pravaah.internal.csv;

public interface CsvRecordSink {
    void startRecord();
    void field(int index, String value);

    /**
     * Optional slice-based field emission. The default implementation
     * falls back to substring + {@link #field(int, String)}, but sinks
     * that only need field presence (count, validation pre-check) can
     * override to avoid the per-field allocation.
     */
    default void field(int index, CharSequence text, int start, int end) {
        field(index, text.subSequence(start, end).toString());
    }

    void endRecord();
}
