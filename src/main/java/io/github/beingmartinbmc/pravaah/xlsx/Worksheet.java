package io.github.beingmartinbmc.pravaah.xlsx;

import io.github.beingmartinbmc.pravaah.Row;

import java.util.*;

public class Worksheet {
    private final String name;
    private final List<Row> rows;
    private final List<ColumnDefinition> columns = new ArrayList<>();
    private final List<String> merges = new ArrayList<>();
    private final List<DataValidation> validations = new ArrayList<>();
    private final List<TableDefinition> tables = new ArrayList<>();
    private FreezePane frozen;

    public Worksheet(String name) {
        this(name, new ArrayList<Row>());
    }

    public Worksheet(String name, List<Row> rows) {
        this.name = name;
        this.rows = rows;
    }

    public String getName() { return name; }
    public List<Row> getRows() { return rows; }
    public List<ColumnDefinition> getColumns() { return columns; }
    public List<String> getMerges() { return merges; }
    public List<DataValidation> getValidations() { return validations; }
    public List<TableDefinition> getTables() { return tables; }

    public FreezePane getFrozen() { return frozen; }
    public void setFrozen(FreezePane f) { this.frozen = f; }
}
