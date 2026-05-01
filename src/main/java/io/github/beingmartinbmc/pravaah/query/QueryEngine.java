package io.github.beingmartinbmc.pravaah.query;

import io.github.beingmartinbmc.pravaah.Row;

import java.util.*;
import java.util.regex.*;

public final class QueryEngine {

    private QueryEngine() {}

    public static List<Row> query(List<Row> source, String sql) {
        if (source == null) throw new IllegalArgumentException("query() requires a source");
        QueryPlan plan = parseQuery(sql);
        List<Row> output = new ArrayList<>();

        for (Row row : source) {
            if (plan.where == null || matchesWhere(row, plan.where)) {
                output.add(project(row, plan.columns));
                if (plan.orderBy == null && plan.limit != null && output.size() >= plan.limit) break;
            }
        }

        List<Row> ordered = plan.orderBy != null ? sortRows(output, plan.orderBy) : output;
        return plan.limit != null && ordered.size() > plan.limit ? ordered.subList(0, plan.limit) : ordered;
    }

    public static Map<String, List<Row>> createIndex(Iterable<Row> rows, String... keys) {
        Map<String, List<Row>> index = new LinkedHashMap<>();
        for (Row row : rows) {
            String id = buildKey(row, keys);
            List<Row> bucket = index.get(id);
            if (bucket == null) {
                bucket = new ArrayList<>();
                index.put(id, bucket);
            }
            bucket.add(row);
        }
        return index;
    }

    public static List<Row> joinRows(Iterable<Row> left, Iterable<Row> right, String... keys) {
        Map<String, List<Row>> index = createIndex(right, keys);
        List<Row> joined = new ArrayList<>();
        for (Row row : left) {
            String id = buildKey(row, keys);
            List<Row> matches = index.get(id);
            if (matches != null) {
                for (Row match : matches) {
                    Row merged = row.copy();
                    merged.putAll(match);
                    joined.add(merged);
                }
            }
        }
        return joined;
    }

    private static String buildKey(Row row, String[] keys) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < keys.length; i++) {
            if (i > 0) sb.append('\0');
            Object v = row.get(keys[i]);
            sb.append(v == null ? "" : String.valueOf(v));
        }
        return sb.toString();
    }

    static QueryPlan parseQuery(String sql) {
        Pattern p = Pattern.compile(
                "^select\\s+(.+?)(?:\\s+where\\s+(.+?))?(?:\\s+order\\s+by\\s+([A-Za-z_][\\w\\s.\\-]*?)(?:\\s+(asc|desc))?)?(?:\\s+limit\\s+(\\d+))?$",
                Pattern.CASE_INSENSITIVE);
        Matcher m = p.matcher(sql.trim());
        if (!m.matches()) throw new IllegalArgumentException("Unsupported query: " + sql);

        String select = m.group(1) != null ? m.group(1) : "*";
        String where = m.group(2);
        String orderColumn = m.group(3);
        String limit = m.group(5);

        List<String> columns;
        if ("*".equals(select.trim())) {
            columns = Collections.singletonList("*");
        } else {
            columns = new ArrayList<>();
            for (String col : select.split(",")) columns.add(col.trim());
        }

        return new QueryPlan(
                columns,
                where != null ? parseWhere(where) : null,
                orderColumn != null ? new OrderByClause(orderColumn.trim(),
                        m.group(4) != null ? m.group(4).toLowerCase() : "asc") : null,
                limit != null ? Integer.parseInt(limit) : null
        );
    }

    private static WhereClause parseWhere(String where) {
        Pattern p = Pattern.compile("^([A-Za-z_][\\w\\s.\\-]*?)\\s*(>=|<=|!=|=|>|<|contains)\\s*(.+)$", Pattern.CASE_INSENSITIVE);
        Matcher m = p.matcher(where.trim());
        if (!m.matches()) throw new IllegalArgumentException("Unsupported WHERE clause: " + where);

        String column = m.group(1).trim();
        String operator = m.group(2).toLowerCase();
        String rawValue = m.group(3).trim();
        Object value = parseLiteral(rawValue);
        return new WhereClause(column, operator, value);
    }

    private static Object parseLiteral(String value) {
        String unquoted = value.replaceAll("^['\"]|['\"]$", "");
        try {
            double d = Double.parseDouble(unquoted);
            if (Double.isFinite(d) && !unquoted.trim().isEmpty()) {
                if (d == Math.floor(d) && !unquoted.contains(".") && d >= Integer.MIN_VALUE && d <= Integer.MAX_VALUE) {
                    return (int) d;
                }
                return d;
            }
        } catch (NumberFormatException ignored) {}
        return unquoted;
    }

    private static boolean matchesWhere(Row row, WhereClause where) {
        Object raw = row.get(where.column);
        Object value = raw instanceof Date ? ((Date) raw).getTime() : raw;
        Object expected = where.value;

        if ("contains".equals(where.operator)) {
            return String.valueOf(value != null ? value : "").contains(String.valueOf(expected));
        }
        if ("=".equals(where.operator)) return Objects.equals(value, expected);
        if ("!=".equals(where.operator)) return !Objects.equals(value, expected);

        if (!(value instanceof Number) || !(expected instanceof Number)) return false;
        double v = ((Number) value).doubleValue();
        double e = ((Number) expected).doubleValue();
        if (">".equals(where.operator)) return v > e;
        if (">=".equals(where.operator)) return v >= e;
        if ("<".equals(where.operator)) return v < e;
        if ("<=".equals(where.operator)) return v <= e;
        return false;
    }

    private static Row project(Row row, List<String> columns) {
        if (columns.size() == 1 && "*".equals(columns.get(0))) return row;
        Row projected = new Row();
        for (String col : columns) {
            projected.put(col, row.containsKey(col) ? row.get(col) : null);
        }
        return projected;
    }

    private static List<Row> sortRows(List<Row> rows, OrderByClause orderBy) {
        List<Row> sorted = new ArrayList<>(rows);
        final int direction = "asc".equals(orderBy.direction) ? 1 : -1;
        Collections.sort(sorted, new Comparator<Row>() {
            @Override
            public int compare(Row left, Row right) {
                Object a = left.get(orderBy.column);
                Object b = right.get(orderBy.column);
                if (a instanceof Number && b instanceof Number) {
                    return Double.compare(((Number) a).doubleValue(), ((Number) b).doubleValue()) * direction;
                }
                return String.valueOf(a != null ? a : "").compareTo(String.valueOf(b != null ? b : "")) * direction;
            }
        });
        return sorted;
    }

    static class QueryPlan {
        final List<String> columns;
        final WhereClause where;
        final OrderByClause orderBy;
        final Integer limit;

        QueryPlan(List<String> columns, WhereClause where, OrderByClause orderBy, Integer limit) {
            this.columns = columns;
            this.where = where;
            this.orderBy = orderBy;
            this.limit = limit;
        }
    }

    static class WhereClause {
        final String column;
        final String operator;
        final Object value;

        WhereClause(String column, String operator, Object value) {
            this.column = column;
            this.operator = operator;
            this.value = value;
        }
    }

    static class OrderByClause {
        final String column;
        final String direction;

        OrderByClause(String column, String direction) {
            this.column = column;
            this.direction = direction;
        }
    }
}
