package io.github.beingmartinbmc.pravaah.internal.csv;

import io.github.beingmartinbmc.pravaah.Row;
import io.github.beingmartinbmc.pravaah.csv.CsvReader;

public final class DefaultRowMaterializer implements RowMaterializer {
    private final String[] headers;
    private final boolean headerless;
    private final boolean inferTypes;
    private Row row;
    private boolean empty;
    private int fieldsSeen;

    public DefaultRowMaterializer(String[] headers, boolean headerless, boolean inferTypes) {
        this.headers = headers;
        this.headerless = headerless;
        this.inferTypes = inferTypes;
    }

    @Override
    public void startRecord() {
        int size = headers != null && !headerless ? headers.length : 8;
        this.row = new Row(mapCapacity(size));
        this.empty = true;
        this.fieldsSeen = 0;
    }

    @Override
    public void field(int index, String value) {
        fieldsSeen = Math.max(fieldsSeen, index + 1);
        if (value != null && !value.isEmpty()) {
            empty = false;
        }

        String key = null;
        if (headerless) {
            key = "_" + (index + 1);
        } else if (headers != null && index < headers.length) {
            key = headers[index];
        }

        if (key != null) {
            row.put(key, inferTypes ? CsvReader.inferValue(value) : value);
        }
    }

    @Override
    public Row finishRecord() {
        if (empty) return null;
        if (!headerless && headers != null) {
            for (int i = fieldsSeen; i < headers.length; i++) {
                row.put(headers[i], null);
            }
        }
        return row;
    }

    private static int mapCapacity(int entries) {
        return Math.max(4, (int) (entries / 0.75f) + 1);
    }
}
