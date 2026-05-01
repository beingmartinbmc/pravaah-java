package io.github.beingmartinbmc.pravaah.internal.io;

import java.io.IOException;
import java.io.InputStream;

public interface InputScanner {
    String readUtf8(InputStream stream, int initialCapacity) throws IOException;
}
