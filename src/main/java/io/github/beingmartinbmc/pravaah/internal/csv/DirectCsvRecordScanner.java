package io.github.beingmartinbmc.pravaah.internal.csv;

/**
 * Inline single-pass CSV scanner. Walks the input string once, emits fields
 * directly to the {@link CsvRecordSink} without any per-field wrapper
 * allocation. Quoted fields use a fast-path that avoids the
 * {@link StringBuilder} when no escapes are present.
 */
public final class DirectCsvRecordScanner implements CsvRecordScanner {
    private static final char CR = '\r';
    private static final char LF = '\n';

    @Override
    public void scan(String text, char delimiter, char quote, CsvRecordSink sink) {
        final int length = text.length();
        int cursor = 0;

        while (cursor < length) {
            sink.startRecord();
            int fieldIndex = 0;
            boolean recordDone = false;

            while (!recordDone) {
                if (cursor >= length) {
                    sink.field(fieldIndex++, "");
                    recordDone = true;
                    break;
                }

                char first = text.charAt(cursor);

                if (first == CR || first == LF) {
                    sink.field(fieldIndex++, text, cursor, cursor);
                    cursor = skipNewline(text, cursor);
                    recordDone = true;
                    break;
                }

                if (first == delimiter) {
                    sink.field(fieldIndex++, text, cursor, cursor);
                    cursor++;
                    continue;
                }

                if (first == quote) {
                    cursor++;
                    int contentStart = cursor;
                    StringBuilder escaped = null;

                    while (cursor < length) {
                        char c = text.charAt(cursor);
                        if (c != quote) {
                            cursor++;
                            continue;
                        }

                        if (cursor + 1 < length && text.charAt(cursor + 1) == quote) {
                            if (escaped == null) {
                                escaped = new StringBuilder(cursor - contentStart + 16);
                            }
                            escaped.append(text, contentStart, cursor).append(quote);
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
                        sink.field(fieldIndex++, value);

                        cursor++;
                        if (cursor >= length) {
                            recordDone = true;
                            break;
                        }
                        char afterQuote = text.charAt(cursor);
                        if (afterQuote == delimiter) {
                            cursor++;
                        } else if (afterQuote == CR || afterQuote == LF) {
                            cursor = skipNewline(text, cursor);
                            recordDone = true;
                        } else {
                            throw new IllegalStateException("Invalid quoted CSV field");
                        }
                        break;
                    }

                    if (cursor >= length && !recordDone && escaped == null) {
                        throw new IllegalStateException("Unclosed quoted CSV field");
                    }
                    continue;
                }

                int fieldStart = cursor;
                boolean emitted = false;
                while (cursor < length) {
                    char c = text.charAt(cursor);
                    if (c == delimiter) {
                        sink.field(fieldIndex++, text, fieldStart, cursor);
                        cursor++;
                        emitted = true;
                        break;
                    }
                    if (c == CR || c == LF) {
                        sink.field(fieldIndex++, text, fieldStart, cursor);
                        cursor = skipNewline(text, cursor);
                        recordDone = true;
                        emitted = true;
                        break;
                    }
                    cursor++;
                }
                if (!emitted) {
                    sink.field(fieldIndex++, text, fieldStart, cursor);
                    recordDone = true;
                }
            }

            sink.endRecord();
        }
    }

    private static int skipNewline(String text, int cursor) {
        if (text.charAt(cursor) == CR && cursor + 1 < text.length() && text.charAt(cursor + 1) == LF) {
            return cursor + 2;
        }
        return cursor + 1;
    }
}
