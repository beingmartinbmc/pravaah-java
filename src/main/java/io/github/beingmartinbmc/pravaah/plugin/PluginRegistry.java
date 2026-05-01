package io.github.beingmartinbmc.pravaah.plugin;

import io.github.beingmartinbmc.pravaah.PravaahIssue;
import io.github.beingmartinbmc.pravaah.Row;

import java.util.*;
import java.util.function.Function;

public class PluginRegistry {

    private final Map<String, PravaahPlugin> plugins = new LinkedHashMap<>();

    public PluginRegistry use(PravaahPlugin plugin) {
        if (plugins.containsKey(plugin.getName())) {
            throw new IllegalStateException("Plugin already registered: " + plugin.getName());
        }
        plugins.put(plugin.getName(), plugin);
        return this;
    }

    public List<PravaahPlugin> list() {
        return new ArrayList<>(plugins.values());
    }

    public Map<String, Function<List<Object>, Object>> formulas() {
        Map<String, Function<List<Object>, Object>> merged = new LinkedHashMap<>();
        for (PravaahPlugin plugin : plugins.values()) {
            if (plugin.getFormulas() != null) {
                merged.putAll(plugin.getFormulas());
            }
        }
        return merged;
    }

    public List<Function<Row, List<PravaahIssue>>> validators() {
        List<Function<Row, List<PravaahIssue>>> all = new ArrayList<>();
        for (PravaahPlugin plugin : plugins.values()) {
            if (plugin.getValidators() != null) {
                all.addAll(plugin.getValidators());
            }
        }
        return all;
    }

    public List<PravaahIssue> validate(Row row) {
        List<PravaahIssue> issues = new ArrayList<>();
        for (Function<Row, List<PravaahIssue>> validator : validators()) {
            issues.addAll(validator.apply(row));
        }
        return issues;
    }

    public List<PravaahIssue> validateRows(Iterable<Row> rows) {
        List<PravaahIssue> issues = new ArrayList<>();
        for (Row row : rows) {
            issues.addAll(validate(row));
        }
        return issues;
    }
}
