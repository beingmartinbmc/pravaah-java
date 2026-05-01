package io.github.beingmartinbmc.pravaah.runtime;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

/**
 * Java 11+ runtime support.
 */
public final class RuntimeSupport {

    private RuntimeSupport() {}

    public static String implementation() {
        return "java11";
    }

    public static String readUtf8(InputStream stream, int initialCapacity) throws IOException {
        try {
            return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
        } finally {
            stream.close();
        }
    }
}
