package io.github.beingmartinbmc.pravaah.xlsx;

public class FormulaCell {
    private final String formula;
    private final Object result;

    public FormulaCell(String formula, Object result) {
        this.formula = formula;
        this.result = result;
    }

    public FormulaCell(String formula) {
        this(formula, null);
    }

    public String getFormula() { return formula; }
    public Object getResult() { return result; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof FormulaCell)) return false;
        FormulaCell that = (FormulaCell) o;
        if (!formula.equals(that.formula)) return false;
        return result != null ? result.equals(that.result) : that.result == null;
    }

    @Override
    public int hashCode() {
        int h = formula.hashCode();
        h = 31 * h + (result != null ? result.hashCode() : 0);
        return h;
    }

    @Override
    public String toString() {
        return "FormulaCell{formula='" + formula + "', result=" + result + "}";
    }
}
