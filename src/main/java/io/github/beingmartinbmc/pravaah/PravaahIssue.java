package io.github.beingmartinbmc.pravaah;

public class PravaahIssue {

    public enum Severity { WARNING, ERROR }

    private final String code;
    private final String message;
    private final Integer rowNumber;
    private final String column;
    private final Object rawValue;
    private final String expected;
    private final Severity severity;

    public PravaahIssue(String code, String message, Integer rowNumber, String column,
                        Object rawValue, String expected, Severity severity) {
        this.code = code;
        this.message = message;
        this.rowNumber = rowNumber;
        this.column = column;
        this.rawValue = rawValue;
        this.expected = expected;
        this.severity = severity;
    }

    public static PravaahIssue error(String code, String message, int rowNumber,
                                     String column, Object rawValue, String expected) {
        return new PravaahIssue(code, message, rowNumber, column, rawValue, expected, Severity.ERROR);
    }

    public static PravaahIssue warning(String code, String message, int rowNumber,
                                       String column, Object rawValue, String expected) {
        return new PravaahIssue(code, message, rowNumber, column, rawValue, expected, Severity.WARNING);
    }

    public String getCode() { return code; }
    public String getMessage() { return message; }
    public Integer getRowNumber() { return rowNumber; }
    public String getColumn() { return column; }
    public Object getRawValue() { return rawValue; }
    public String getExpected() { return expected; }
    public Severity getSeverity() { return severity; }
}
