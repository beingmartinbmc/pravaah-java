package io.github.beingmartinbmc.pravaah.internal.csv;

public interface CsvRecordSink {
    void startRecord();
    void field(int index, String value);
    void endRecord();
}
