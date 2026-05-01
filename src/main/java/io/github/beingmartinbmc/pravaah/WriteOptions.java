package io.github.beingmartinbmc.pravaah;

import java.util.List;

public class WriteOptions {
    private PravaahFormat format;
    private String sheetName;
    private List<String> headers;
    private String delimiter = ",";

    public PravaahFormat getFormat() { return format; }
    public WriteOptions format(PravaahFormat f) { this.format = f; return this; }

    public String getSheetName() { return sheetName; }
    public WriteOptions sheetName(String s) { this.sheetName = s; return this; }

    public List<String> getHeaders() { return headers; }
    public WriteOptions headers(List<String> h) { this.headers = h; return this; }

    public String getDelimiter() { return delimiter; }
    public WriteOptions delimiter(String d) { this.delimiter = d; return this; }

    public static WriteOptions defaults() { return new WriteOptions(); }
}
