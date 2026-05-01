package io.github.beingmartinbmc.pravaah.runtime;

import io.github.beingmartinbmc.pravaah.internal.io.InputScanner;
import io.github.beingmartinbmc.pravaah.internal.io.Java8Utf8Reader;
import io.github.beingmartinbmc.pravaah.internal.io.Utf8Reader;

import java.io.IOException;
import java.io.InputStream;

/**
 * Java 8 baseline runtime support.
 *
 * Multi-release builds replace this class on newer JVMs from
 * META-INF/versions/{11,17}.
 */
public final class RuntimeSupport {

    private static final Java8Utf8Reader UTF8_READER = new Java8Utf8Reader();

    private RuntimeSupport() {}

    public static String implementation() {
        return "java8";
    }

    public static Utf8Reader utf8Reader() {
        return UTF8_READER;
    }

    public static InputScanner inputScanner() {
        return UTF8_READER;
    }

    public static String readUtf8(InputStream stream, int initialCapacity) throws IOException {
        return UTF8_READER.readUtf8(stream, initialCapacity);
    }
}
