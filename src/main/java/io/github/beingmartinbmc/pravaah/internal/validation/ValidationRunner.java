package io.github.beingmartinbmc.pravaah.internal.validation;

import io.github.beingmartinbmc.pravaah.ProcessResult;
import io.github.beingmartinbmc.pravaah.Row;

public interface ValidationRunner {
    void accept(Row row, int rowNumber);
    ProcessResult finish();
}
