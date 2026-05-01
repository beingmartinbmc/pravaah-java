package io.github.beingmartinbmc.pravaah.xlsx;

public class FreezePane {
    private Integer xSplit;
    private Integer ySplit;
    private String topLeftCell;

    public FreezePane() {}

    public FreezePane(Integer xSplit, Integer ySplit, String topLeftCell) {
        this.xSplit = xSplit;
        this.ySplit = ySplit;
        this.topLeftCell = topLeftCell;
    }

    public Integer getXSplit() { return xSplit; }
    public FreezePane xSplit(int x) { this.xSplit = x; return this; }

    public Integer getYSplit() { return ySplit; }
    public FreezePane ySplit(int y) { this.ySplit = y; return this; }

    public String getTopLeftCell() { return topLeftCell; }
    public FreezePane topLeftCell(String c) { this.topLeftCell = c; return this; }
}
