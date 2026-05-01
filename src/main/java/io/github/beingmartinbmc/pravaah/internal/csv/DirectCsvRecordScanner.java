package io.github.beingmartinbmc.pravaah.internal.csv;

public final class DirectCsvRecordScanner implements CsvRecordScanner {
    private static final char QUOTE = '"';
    private static final char CR = '\r';
    private static final char LF = '\n';

    @Override
    public void scan(String text, char delimiter, CsvRecordSink sink) {
        int cursor = 0;
        int length = text.length();

        while (cursor < length) {
            sink.startRecord();
            int fieldIndex = 0;
            boolean endRecord = false;

            while (!endRecord) {
                Field field = readField(text, cursor, delimiter);
                sink.field(fieldIndex++, field.value);
                cursor = field.nextCursor;
                endRecord = field.endRecord;
            }

            sink.endRecord();
        }
    }

    private Field readField(String text, int start, char delimiter) {
        int length = text.length();
        if (start >= length) {
            return new Field("", start, true);
        }

        char first = text.charAt(start);
        if (first == delimiter) {
            return new Field("", start + 1, false);
        }
        if (first == CR || first == LF) {
            return new Field("", skipNewline(text, start), true);
        }
        if (first == QUOTE) {
            return readQuotedField(text, start, delimiter);
        }
        return readUnquotedField(text, start, delimiter);
    }

    private Field readUnquotedField(String text, int start, char delimiter) {
        int cursor = start;
        int length = text.length();
        while (cursor < length) {
            char c = text.charAt(cursor);
            if (c == delimiter) {
                return new Field(text.substring(start, cursor), cursor + 1, false);
            }
            if (c == CR || c == LF) {
                return new Field(text.substring(start, cursor), skipNewline(text, cursor), true);
            }
            cursor++;
        }
        return new Field(text.substring(start, cursor), cursor, true);
    }

    private Field readQuotedField(String text, int start, char delimiter) {
        int cursor = start + 1;
        int contentStart = cursor;
        int length = text.length();
        StringBuilder escaped = null;

        while (cursor < length) {
            char c = text.charAt(cursor);
            if (c != QUOTE) {
                cursor++;
                continue;
            }

            if (cursor + 1 < length && text.charAt(cursor + 1) == QUOTE) {
                if (escaped == null) {
                    escaped = new StringBuilder(cursor - contentStart + 16);
                }
                escaped.append(text, contentStart, cursor).append(QUOTE);
                cursor += 2;
                contentStart = cursor;
                continue;
            }

            String value;
            if (escaped == null) {
                value = text.substring(contentStart, cursor);
            } else {
                escaped.append(text, contentStart, cursor);
                value = escaped.toString();
            }

            cursor++;
            if (cursor >= length) {
                return new Field(value, cursor, true);
            }

            char afterQuote = text.charAt(cursor);
            if (afterQuote == delimiter) {
                return new Field(value, cursor + 1, false);
            }
            if (afterQuote == CR || afterQuote == LF) {
                return new Field(value, skipNewline(text, cursor), true);
            }
            throw new IllegalStateException("Invalid quoted CSV field");
        }

        throw new IllegalStateException("Unclosed quoted CSV field");
    }

    private static int skipNewline(String text, int cursor) {
        if (text.charAt(cursor) == CR && cursor + 1 < text.length() && text.charAt(cursor + 1) == LF) {
            return cursor + 2;
        }
        return cursor + 1;
    }

    private static final class Field {
        final String value;
        final int nextCursor;
        final boolean endRecord;

        Field(String value, int nextCursor, boolean endRecord) {
            this.value = value;
            this.nextCursor = nextCursor;
            this.endRecord = endRecord;
        }
    }
}
