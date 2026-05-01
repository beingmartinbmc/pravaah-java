package io.github.beingmartinbmc.pravaah.formula;

import io.github.beingmartinbmc.pravaah.Row;

import java.util.*;
import java.util.function.BiFunction;
import java.util.regex.*;

public class FormulaEngine {

    @FunctionalInterface
    public interface FormulaFunction {
        Object apply(List<Object> args, Row row);
    }

    private final Map<String, FormulaFunction> functions = new LinkedHashMap<>();

    public FormulaEngine() {
        registerDefaults();
    }

    public FormulaEngine(Map<String, FormulaFunction> customFunctions) {
        registerDefaults();
        if (customFunctions != null) {
            for (Map.Entry<String, FormulaFunction> entry : customFunctions.entrySet()) {
                register(entry.getKey(), entry.getValue());
            }
        }
    }

    public FormulaEngine register(String name, FormulaFunction fn) {
        functions.put(name.toUpperCase(), fn);
        return this;
    }

    public Object evaluate(String formula, Row row) {
        String source = formula.trim();
        if (source.startsWith("=")) source = source.substring(1);

        Matcher call = Pattern.compile("^([A-Z][A-Z0-9.]*?)\\((.*)\\)$", Pattern.CASE_INSENSITIVE).matcher(source);
        if (!call.matches()) return evaluateExpression(source, row);

        String name = call.group(1);
        String argsSource = call.group(2);
        FormulaFunction fn = functions.get(name.toUpperCase());
        if (fn == null) throw new IllegalArgumentException("Unsupported formula function: " + name);

        List<Object> args = new ArrayList<>();
        for (String arg : splitArgs(argsSource)) {
            args.add(resolveArg(arg, row));
        }
        return fn.apply(args, row);
    }

    public Object evaluate(String formula) {
        return evaluate(formula, new Row());
    }

    public static Object evaluateFormula(String formula, Row row) {
        return new FormulaEngine().evaluate(formula, row);
    }

    public static Object evaluateFormula(String formula) {
        return evaluateFormula(formula, new Row());
    }

    private void registerDefaults() {
        register("SUM", (args, row) -> {
            double sum = 0;
            for (double n : toNumbers(args)) sum += n;
            return sum;
        });
        register("AVERAGE", (args, row) -> {
            List<Double> nums = toNumbers(args);
            if (nums.isEmpty()) return 0.0;
            double sum = 0;
            for (double n : nums) sum += n;
            return sum / nums.size();
        });
        register("MIN", (args, row) -> {
            List<Double> nums = toNumbers(args);
            double min = Double.MAX_VALUE;
            for (double n : nums) if (n < min) min = n;
            return min;
        });
        register("MAX", (args, row) -> {
            List<Double> nums = toNumbers(args);
            double max = -Double.MAX_VALUE;
            for (double n : nums) if (n > max) max = n;
            return max;
        });
        register("COUNT", (args, row) -> toNumbers(args).size());
        register("IF", (args, row) -> {
            boolean cond = isTruthy(args.size() > 0 ? args.get(0) : null);
            return cond ? (args.size() > 1 ? args.get(1) : null) : (args.size() > 2 ? args.get(2) : null);
        });
        register("CONCAT", (args, row) -> {
            StringBuilder sb = new StringBuilder();
            for (Object arg : args) sb.append(valueToString(arg));
            return sb.toString();
        });
    }

    private Object evaluateExpression(String source, Row row) {
        String replaced = replaceVariables(source, row);
        if (!replaced.matches("^[\\d+\\-*/().\\s]+$")) return source;
        try {
            return evalArithmetic(replaced);
        } catch (Exception e) {
            return source;
        }
    }

    private String replaceVariables(String source, Row row) {
        Matcher m = Pattern.compile("\\b[A-Za-z_][A-Za-z0-9_]*\\b").matcher(source);
        StringBuffer sb = new StringBuffer();
        while (m.find()) {
            String token = m.group();
            Object value = row.get(token);
            if (value instanceof Number) {
                m.appendReplacement(sb, Matcher.quoteReplacement(String.valueOf(value)));
            } else {
                m.appendReplacement(sb, Matcher.quoteReplacement(token));
            }
        }
        m.appendTail(sb);
        return sb.toString();
    }

    private double evalArithmetic(String expr) {
        return new ArithmeticParser(expr.trim()).parseExpression();
    }

    static List<String> splitArgs(String value) {
        List<String> args = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        Character quote = null;
        int depth = 0;

        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if ((c == '"' || c == '\'') && quote == null) {
                quote = c;
            } else if (quote != null && c == quote) {
                quote = null;
            } else if (c == '(' && quote == null) {
                depth++;
            } else if (c == ')' && quote == null) {
                depth--;
            }

            if (c == ',' && quote == null && depth == 0) {
                args.add(current.toString().trim());
                current = new StringBuilder();
            } else {
                current.append(c);
            }
        }
        if (current.toString().trim().length() > 0) args.add(current.toString().trim());
        return args;
    }

    private Object resolveArg(String arg, Row row) {
        String trimmed = arg.trim();
        if ((trimmed.startsWith("\"") && trimmed.endsWith("\"")) || (trimmed.startsWith("'") && trimmed.endsWith("'"))) {
            return trimmed.substring(1, trimmed.length() - 1);
        }
        if (row.containsKey(trimmed)) {
            Object v = row.get(trimmed);
            return v != null ? v : null;
        }
        try {
            double d = Double.parseDouble(trimmed);
            if (Double.isFinite(d)) return d;
        } catch (NumberFormatException ignored) {}
        if ("true".equalsIgnoreCase(trimmed)) return true;
        if ("false".equalsIgnoreCase(trimmed)) return false;
        return trimmed;
    }

    private static List<Double> toNumbers(List<Object> args) {
        List<Double> result = new ArrayList<>();
        for (Object arg : args) {
            if (arg instanceof List) {
                result.addAll(toNumbers((List<Object>) arg));
                continue;
            }
            Double d = toDouble(arg);
            if (d != null && Double.isFinite(d)) result.add(d);
        }
        return result;
    }

    private static Double toDouble(Object value) {
        if (value instanceof Number) return ((Number) value).doubleValue();
        if (value instanceof String) {
            try {
                return Double.parseDouble((String) value);
            } catch (NumberFormatException e) {
                return null;
            }
        }
        return null;
    }

    private static boolean isTruthy(Object value) {
        if (value == null) return false;
        if (value instanceof Boolean) return (Boolean) value;
        if (value instanceof Number) return ((Number) value).doubleValue() != 0;
        if (value instanceof String) return !((String) value).isEmpty();
        if (value instanceof List) return !((List<?>) value).isEmpty();
        return true;
    }

    private static String valueToString(Object value) {
        if (value == null) return "";
        if (value instanceof Date) return ((Date) value).toInstant().toString();
        return String.valueOf(value);
    }

    private static class ArithmeticParser {
        private final String expr;
        private int pos = 0;

        ArithmeticParser(String expr) { this.expr = expr; }

        double parseExpression() {
            double result = parseTerm();
            while (pos < expr.length()) {
                skipWhitespace();
                if (pos >= expr.length()) break;
                char c = expr.charAt(pos);
                if (c == '+') { pos++; result += parseTerm(); }
                else if (c == '-') { pos++; result -= parseTerm(); }
                else break;
            }
            return result;
        }

        double parseTerm() {
            double result = parseFactor();
            while (pos < expr.length()) {
                skipWhitespace();
                if (pos >= expr.length()) break;
                char c = expr.charAt(pos);
                if (c == '*') { pos++; result *= parseFactor(); }
                else if (c == '/') { pos++; result /= parseFactor(); }
                else break;
            }
            return result;
        }

        double parseFactor() {
            skipWhitespace();
            if (pos < expr.length() && expr.charAt(pos) == '(') {
                pos++;
                double result = parseExpression();
                skipWhitespace();
                if (pos < expr.length() && expr.charAt(pos) == ')') pos++;
                return result;
            }
            if (pos < expr.length() && expr.charAt(pos) == '-') {
                pos++;
                return -parseFactor();
            }
            int start = pos;
            while (pos < expr.length() && (Character.isDigit(expr.charAt(pos)) || expr.charAt(pos) == '.')) {
                pos++;
            }
            return Double.parseDouble(expr.substring(start, pos));
        }

        void skipWhitespace() {
            while (pos < expr.length() && expr.charAt(pos) == ' ') pos++;
        }
    }
}
