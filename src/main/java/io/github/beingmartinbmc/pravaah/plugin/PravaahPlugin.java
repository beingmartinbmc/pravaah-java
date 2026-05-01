package io.github.beingmartinbmc.pravaah.plugin;

import io.github.beingmartinbmc.pravaah.PravaahIssue;
import io.github.beingmartinbmc.pravaah.Row;

import java.util.*;
import java.util.function.Function;

public class PravaahPlugin {
    private final String name;
    private Map<String, Function<List<Object>, Object>> formulas;
    private List<Function<Row, List<PravaahIssue>>> validators;

    public PravaahPlugin(String name) {
        this.name = name;
    }

    public String getName() { return name; }

    public Map<String, Function<List<Object>, Object>> getFormulas() { return formulas; }
    public PravaahPlugin formulas(Map<String, Function<List<Object>, Object>> f) { this.formulas = f; return this; }

    public List<Function<Row, List<PravaahIssue>>> getValidators() { return validators; }
    public PravaahPlugin validators(List<Function<Row, List<PravaahIssue>>> v) { this.validators = v; return this; }
}
