package io.github.beingmartinbmc.pravaah.xlsx;

import java.util.*;

public class Workbook {
    private final List<Worksheet> sheets;
    private final Map<String, String> properties;

    public Workbook() {
        this(new ArrayList<Worksheet>());
    }

    public Workbook(List<Worksheet> sheets) {
        this.sheets = sheets;
        this.properties = new LinkedHashMap<>();
    }

    public List<Worksheet> getSheets() { return sheets; }
    public Map<String, String> getProperties() { return properties; }
}
