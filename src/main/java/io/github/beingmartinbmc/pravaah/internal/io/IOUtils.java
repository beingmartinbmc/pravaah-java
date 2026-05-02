package io.github.beingmartinbmc.pravaah.internal.io;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Centralised IO helpers and buffer-size constants used across the readers and writers.
 *
 * <p>Pravaah avoids depending on Apache Commons IO (or any third-party utility library) so we
 * keep the abstractions tiny and aligned with the patterns each format reader needs.</p>
 */
public final class IOUtils {

    /** Default buffer size for stream-to-byte copy loops (8 KiB). */
    public static final int STREAM_COPY_BUFFER_SIZE = 8 * 1024;

    /** Default buffer size for character / chunk reads (4 KiB). */
    public static final int CHAR_READ_BUFFER_SIZE = 4 * 1024;

    /** Default buffer size for high-throughput byte sinks (64 KiB). */
    public static final int LARGE_IO_BUFFER_SIZE = 64 * 1024;

    private IOUtils() {}

    /** Reads all bytes from {@code path} using try-with-resources for the underlying stream. */
    public static byte[] readAllBytes(String path) throws IOException {
        try (FileInputStream fis = new FileInputStream(path)) {
            return readAllBytes(fis);
        }
    }

    /**
     * Reads the supplied stream to completion into a byte array. The caller retains ownership
     * of the stream and is responsible for closing it.
     */
    public static byte[] readAllBytes(InputStream is) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        byte[] buf = new byte[STREAM_COPY_BUFFER_SIZE];
        int read;
        while ((read = is.read(buf)) != -1) {
            baos.write(buf, 0, read);
        }
        return baos.toByteArray();
    }
}
