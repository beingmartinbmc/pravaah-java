package io.github.beingmartinbmc.pravaah.runtime;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;

/**
 * Java 8 baseline runtime support.
 *
 * Multi-release builds replace this class on newer JVMs from
 * META-INF/versions/{11,17}.
 */
public final class RuntimeSupport {

    private RuntimeSupport() {}

    public static String implementation() {
        return "java8";
    }

    public static String readUtf8(InputStream stream, int initialCapacity) throws IOException {
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
