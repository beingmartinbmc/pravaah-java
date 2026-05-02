package io.github.beingmartinbmc.pravaah.internal.csv;

import io.github.beingmartinbmc.pravaah.Row;
import io.github.beingmartinbmc.pravaah.csv.CsvReader;
import io.github.beingmartinbmc.pravaah.internal.util.Maps;

public final class DefaultRowMaterializer implements RowMaterializer {

    /** Capacity used for headerless rows where we cannot pre-size by header count. */
    private static final int HEADERLESS_DEFAULT_SIZE = 8;

    private final String[] headers;
    private final boolean headerless;
    private final boolean inferTypes;
    private final int headerCount;
    private final int rowInitialCapacity;
    private Row row;
    private boolean empty;
    private int fieldsSeen;

    public DefaultRowMaterializer(String[] headers, boolean headerless, boolean inferTypes) {
        this.headers = headers;
        this.headerless = headerless;
        this.inferTypes = inferTypes;
        this.headerCount = headers != null && !headerless ? headers.length : 0;
        int size = headerCount > 0 ? headerCount : HEADERLESS_DEFAULT_SIZE;
        this.rowInitialCapacity = Maps.mapCapacity(size);
    }

    @Override
    public void startRecord() {
        this.row = new Row(rowInitialCapacity);
        this.empty = true;
        this.fieldsSeen = 0;
    }

    @Override
    public void field(int index, String value) {
        fieldsSeen = index + 1;
        if (empty && value != null && !value.isEmpty()) {
            empty = false;
        }

        String key;
        if (headerless) {
            key = "_" + (index + 1);
        } else if (headers != null && index < headerCount) {
            key = headers[index];
        } else {
            return;
        }

        row.put(key, inferTypes ? CsvReader.inferValue(value) : value);
    }

    /**
     * Fast path for an empty field: avoids allocating an empty {@code String}
     * substring at the scanner level. Caller must already know the field is
     * empty.
     */
    public void fieldEmpty(int index) {
        fieldsSeen = index + 1;
        String key;
        if (headerless) {
            key = "_" + (index + 1);
        } else if (headers != null && index < headerCount) {
            key = headers[index];
        } else {
            return;
        }
        row.put(key, inferTypes ? null : "");
    }

    @Override
    public Row finishRecord() {
        if (empty) return null;
        if (!headerless && headers != null) {
            for (int i = fieldsSeen; i < headerCount; i++) {
                row.put(headers[i], null);
            }
        }
        return row;
    }

}
