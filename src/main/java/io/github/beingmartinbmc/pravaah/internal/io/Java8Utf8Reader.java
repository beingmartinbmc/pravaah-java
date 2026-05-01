package io.github.beingmartinbmc.pravaah.internal.io;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;

public final class Java8Utf8Reader implements Utf8Reader, InputScanner {
    @Override
    public String read(InputStream stream, int initialCapacity) throws IOException {
        return readUtf8(stream, initialCapacity);
    }

    @Override
    public String readUtf8(InputStream stream, int initialCapacity) throws IOException {
        Reader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
        try {
            StringBuilder sb = new StringBuilder(Math.max(16, initialCapacity));
            char[] buffer = new char[8192];
            int read;
            while ((read = reader.read(buffer)) != -1) {
                sb.append(buffer, 0, read);
            }
            return sb.toString();
        } finally {
            reader.close();
        }
    }
}
