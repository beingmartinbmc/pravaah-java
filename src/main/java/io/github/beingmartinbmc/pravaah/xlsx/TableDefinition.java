package io.github.beingmartinbmc.pravaah.xlsx;

import java.util.List;

public class TableDefinition {
    private final String name;
    private final String range;
    private final List<String> columns;

    public TableDefinition(String name, String range, List<String> columns) {
        this.name = name;
        this.range = range;
        this.columns = columns;
    }

    public String getName() { return name; }
    public String getRange() { return range; }
    public List<String> getColumns() { return columns; }
}
