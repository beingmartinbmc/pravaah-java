package io.github.beingmartinbmc.pravaah;

import java.util.List;

public class ReadOptions {
    private PravaahFormat format;
    private String sheetName;
    private Integer sheetIndex;
    private Boolean headers = true;
    private List<String> headerNames;
    private String delimiter = ",";
    private boolean inferTypes;
    private String formulas = "values";
    private ValidationMode validation;
    private CleaningOptions cleaning;

    public PravaahFormat getFormat() { return format; }
    public ReadOptions format(PravaahFormat f) { this.format = f; return this; }

    public String getSheetName() { return sheetName; }
    public ReadOptions sheetName(String s) { this.sheetName = s; return this; }

    public Integer getSheetIndex() { return sheetIndex; }
    public ReadOptions sheetIndex(int i) { this.sheetIndex = i; return this; }

    public Boolean getHeaders() { return headers; }
    public ReadOptions headers(boolean h) { this.headers = h; return this; }

    public List<String> getHeaderNames() { return headerNames; }
    public ReadOptions headerNames(List<String> h) { this.headerNames = h; return this; }

    public String getDelimiter() { return delimiter; }
    public ReadOptions delimiter(String d) { this.delimiter = d; return this; }

    public boolean isInferTypes() { return inferTypes; }
    public ReadOptions inferTypes(boolean i) { this.inferTypes = i; return this; }

    public String getFormulas() { return formulas; }
    public ReadOptions formulas(String f) { this.formulas = f; return this; }

    public ValidationMode getValidation() { return validation; }
    public ReadOptions validation(ValidationMode v) { this.validation = v; return this; }

    public CleaningOptions getCleaning() { return cleaning; }
    public ReadOptions cleaning(CleaningOptions c) { this.cleaning = c; return this; }

    public static ReadOptions defaults() { return new ReadOptions(); }
}
