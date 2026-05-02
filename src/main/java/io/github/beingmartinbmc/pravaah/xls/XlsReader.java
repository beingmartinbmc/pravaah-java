package io.github.beingmartinbmc.pravaah.xls;

import io.github.beingmartinbmc.pravaah.ReadOptions;
import io.github.beingmartinbmc.pravaah.Row;
import io.github.beingmartinbmc.pravaah.internal.io.IOUtils;
import io.github.beingmartinbmc.pravaah.xlsx.Workbook;

import java.io.IOException;
import java.util.List;

public final class XlsReader {
    private XlsReader() {}

    public static List<Row> readAll(String filePath, ReadOptions options) throws IOException {
        return readAll(IOUtils.readAllBytes(filePath), options);
    }

    public static List<Row> readAll(byte[] data, ReadOptions options) throws IOException {
        byte[] workbook = CompoundFileReader.readStream(data, "Workbook", "Book");
        return new Biff8Reader(workbook).readAll(options);
    }

    public static Workbook readWorkbook(String filePath, ReadOptions options) throws IOException {
        return readWorkbook(IOUtils.readAllBytes(filePath), options);
    }

    public static Workbook readWorkbook(byte[] data, ReadOptions options) throws IOException {
        byte[] workbook = CompoundFileReader.readStream(data, "Workbook", "Book");
        return new Biff8Reader(workbook).readWorkbook(options);
    }
}
