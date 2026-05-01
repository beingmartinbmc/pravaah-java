package io.github.beingmartinbmc.pravaah.internal.csv;

public interface CsvRecordScanner {
    void scan(String text, char delimiter, CsvRecordSink sink);
}
