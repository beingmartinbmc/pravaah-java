package io.github.beingmartinbmc.pravaah.internal.csv;

import io.github.beingmartinbmc.pravaah.Row;

public interface RowMaterializer {
    void startRecord();
    void field(int index, String value);
    Row finishRecord();
}
