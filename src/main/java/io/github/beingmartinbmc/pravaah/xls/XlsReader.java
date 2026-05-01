package io.github.beingmartinbmc.pravaah.xls;

import io.github.beingmartinbmc.pravaah.ReadOptions;
import io.github.beingmartinbmc.pravaah.Row;
import io.github.beingmartinbmc.pravaah.xlsx.Workbook;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public final class XlsReader {
    private XlsReader() {}

    public static List<Row> readAll(String filePath, ReadOptions options) throws IOException {
        return readAll(readBytes(filePath), options);
    }

    public static List<Row> readAll(byte[] data, ReadOptions options) throws IOException {
        byte[] workbook = CompoundFileReader.readStream(data, "Workbook", "Book");
        return new Biff8Reader(workbook).readAll(options);
    }

    public static Workbook readWorkbook(String filePath, ReadOptions options) throws IOException {
        return readWorkbook(readBytes(filePath), options);
    }

    public static Workbook readWorkbook(byte[] data, ReadOptions options) throws IOException {
        byte[] workbook = CompoundFileReader.readStream(data, "Workbook", "Book");
        return new Biff8Reader(workbook).readWorkbook(options);
    }

    private static byte[] readBytes(String path) throws IOException {
        FileInputStream fis = new FileInputStream(path);
        try {
            return readStreamFully(fis);
        } finally {
            fis.close();
        }
    }

    private static byte[] readStreamFully(InputStream is) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        byte[] buf = new byte[8192];
        int read;
        while ((read = is.read(buf)) != -1) {
            baos.write(buf, 0, read);
        }
        return baos.toByteArray();
    }
}
