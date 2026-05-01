package io.github.beingmartinbmc.pravaah.csv;

import io.github.beingmartinbmc.pravaah.Row;

public interface RowConsumer {
    void accept(Row row, int rowNumber);
}
