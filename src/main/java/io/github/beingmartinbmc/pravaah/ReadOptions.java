package io.github.beingmartinbmc.pravaah;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.IntConsumer;

public class ReadOptions {
    private PravaahFormat format;
    private String sheetName;
    private Integer sheetIndex;
    private Boolean headers = true;
    private List<String> headerNames;
    private String delimiter = ",";
    private String quote = "\"";
    private boolean autoDetectDelimiter;
    private boolean autoDetectEncoding = true;
    private boolean autoDetectQuote;
    private Charset charset = StandardCharsets.UTF_8;
    private boolean inferTypes;
    private String formulas = "values";
    private ValidationMode validation;
    private CleaningOptions cleaning;
    private boolean strictHeaders;
    private IntConsumer progressConsumer;
    private Consumer<PravaahIssue> issueConsumer;

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

    public String getQuote() { return quote; }
    public ReadOptions quote(String q) { this.quote = q; return this; }

    public boolean isAutoDetectDelimiter() { return autoDetectDelimiter; }
    public ReadOptions autoDetectDelimiter(boolean a) { this.autoDetectDelimiter = a; return this; }

    public boolean isAutoDetectEncoding() { return autoDetectEncoding; }
    public ReadOptions autoDetectEncoding(boolean a) { this.autoDetectEncoding = a; return this; }

    public boolean isAutoDetectQuote() { return autoDetectQuote; }
    public ReadOptions autoDetectQuote(boolean a) { this.autoDetectQuote = a; return this; }

    public Charset getCharset() { return charset; }
    public ReadOptions charset(Charset c) { this.charset = c; return this; }
    public ReadOptions charset(String c) { this.charset = Charset.forName(c); return this; }

    public boolean isInferTypes() { return inferTypes; }
    public ReadOptions inferTypes(boolean i) { this.inferTypes = i; return this; }

    public String getFormulas() { return formulas; }
    public ReadOptions formulas(String f) { this.formulas = f; return this; }

    public ValidationMode getValidation() { return validation; }
    public ReadOptions validation(ValidationMode v) { this.validation = v; return this; }

    public CleaningOptions getCleaning() { return cleaning; }
    public ReadOptions cleaning(CleaningOptions c) { this.cleaning = c; return this; }

    public boolean isStrictHeaders() { return strictHeaders; }
    public ReadOptions strictHeaders(boolean s) { this.strictHeaders = s; return this; }

    public IntConsumer getProgressConsumer() { return progressConsumer; }
    public ReadOptions onProgress(IntConsumer c) { this.progressConsumer = c; return this; }

    public Consumer<PravaahIssue> getIssueConsumer() { return issueConsumer; }
    public ReadOptions onIssue(Consumer<PravaahIssue> c) { this.issueConsumer = c; return this; }

    public static ReadOptions defaults() { return new ReadOptions(); }
}
