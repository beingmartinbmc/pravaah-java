package io.github.beingmartinbmc.pravaah.xlsx;

public class DataValidation {
    private final String range;
    private final String type;
    private final String formula;

    public DataValidation(String range, String type, String formula) {
        this.range = range;
        this.type = type;
        this.formula = formula;
    }

    public String getRange() { return range; }
    public String getType() { return type; }
    public String getFormula() { return formula; }
}
