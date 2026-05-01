package io.github.beingmartinbmc.pravaah.internal.io;

import java.io.IOException;
import java.io.InputStream;

public interface Utf8Reader {
    String read(InputStream stream, int initialCapacity) throws IOException;
}
