package io.github.beingmartinbmc.pravaah.xlsx;

public class ColumnDefinition {
    private String header;
    private String key;
    private int width = 12;

    public ColumnDefinition() {}

    public ColumnDefinition(String header) {
        this.header = header;
    }

    public ColumnDefinition(String header, int width) {
        this.header = header;
        this.width = width;
    }

    public String getHeader() { return header; }
    public ColumnDefinition header(String h) { this.header = h; return this; }

    public String getKey() { return key; }
    public ColumnDefinition key(String k) { this.key = k; return this; }

    public int getWidth() { return width; }
    public ColumnDefinition width(int w) { this.width = w; return this; }
}
