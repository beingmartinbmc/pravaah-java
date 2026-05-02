package io.github.beingmartinbmc.pravaah.internal.io;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;

public final class Java8Utf8Reader implements Utf8Reader, InputScanner {

    private static final int MIN_BUILDER_CAPACITY = 16;

    @Override
    public String read(InputStream stream, int initialCapacity) throws IOException {
        return readUtf8(stream, initialCapacity);
    }

    @Override
    public String readUtf8(InputStream stream, int initialCapacity) throws IOException {
        try (Reader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
            StringBuilder sb = new StringBuilder(Math.max(MIN_BUILDER_CAPACITY, initialCapacity));
            char[] buffer = new char[IOUtils.STREAM_COPY_BUFFER_SIZE];
            int read;
            while ((read = reader.read(buffer)) != -1) {
                sb.append(buffer, 0, read);
            }
            return sb.toString();
        }
    }
}
