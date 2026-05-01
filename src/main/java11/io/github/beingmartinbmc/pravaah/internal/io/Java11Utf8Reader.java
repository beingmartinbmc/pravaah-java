package io.github.beingmartinbmc.pravaah.internal.io;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public final class Java11Utf8Reader implements Utf8Reader, InputScanner {
    @Override
    public String read(InputStream stream, int initialCapacity) throws IOException {
        return readUtf8(stream, initialCapacity);
    }

    @Override
    public String readUtf8(InputStream stream, int initialCapacity) throws IOException {
        try {
            return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
        } finally {
            stream.close();
        }
    }
}
