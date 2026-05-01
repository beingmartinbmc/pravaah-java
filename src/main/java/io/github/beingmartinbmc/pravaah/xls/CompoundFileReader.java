package io.github.beingmartinbmc.pravaah.xls;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

final class CompoundFileReader {
    private static final byte[] SIGNATURE = new byte[] {
            (byte) 0xD0, (byte) 0xCF, 0x11, (byte) 0xE0, (byte) 0xA1, (byte) 0xB1, 0x1A, (byte) 0xE1
    };
    private static final int FREESECT = -1;
    private static final int ENDOFCHAIN = -2;
    private static final int FATSECT = -3;
    private static final int DIFSECT = -4;

    private final byte[] data;
    private final int sectorSize;
    private final int miniSectorSize;
    private final int miniStreamCutoff;
    private final int firstDirectorySector;
    private final int firstMiniFatSector;
    private final int miniFatSectorCount;
    private final int firstDifatSector;
    private final int difatSectorCount;
    private final List<Integer> difat;
    private final int[] fat;
    private final List<DirectoryEntry> directory;
    private final DirectoryEntry root;
    private final int[] miniFat;
    private final byte[] miniStream;

    private CompoundFileReader(byte[] data) throws IOException {
        if (data.length < 512 || !hasSignature(data)) {
            throw new IOException("Not an OLE2 compound document");
        }
        this.data = data;
        int sectorShift = u16(data, 30);
        int miniSectorShift = u16(data, 32);
        this.sectorSize = 1 << sectorShift;
        this.miniSectorSize = 1 << miniSectorShift;
        this.firstDirectorySector = i32(data, 48);
        this.miniStreamCutoff = i32(data, 56);
        this.firstMiniFatSector = i32(data, 60);
        this.miniFatSectorCount = i32(data, 64);
        this.firstDifatSector = i32(data, 68);
        this.difatSectorCount = i32(data, 72);
        this.difat = readDifat();
        this.fat = readFat();
        byte[] directoryBytes = readRegularChain(firstDirectorySector, -1);
        this.directory = parseDirectory(directoryBytes);
        this.root = findRoot(directory);
        this.miniFat = readMiniFat();
        this.miniStream = root != null && root.startSector >= 0
                ? readRegularChain(root.startSector, (int) root.streamSize)
                : new byte[0];
    }

    static byte[] readStream(byte[] data, String... names) throws IOException {
        CompoundFileReader reader = new CompoundFileReader(data);
        for (String name : names) {
            byte[] stream = reader.stream(name);
            if (stream != null) return stream;
        }
        throw new IOException("Workbook stream not found");
    }

    private byte[] stream(String name) throws IOException {
        DirectoryEntry entry = null;
        for (DirectoryEntry e : directory) {
            if (e.type == 2 && e.name.equalsIgnoreCase(name)) {
                entry = e;
                break;
            }
        }
        if (entry == null) return null;
        if (entry.streamSize < miniStreamCutoff && entry.startSector >= 0 && miniFat.length > 0) {
            return readMiniChain(entry.startSector, (int) entry.streamSize);
        }
        return readRegularChain(entry.startSector, (int) entry.streamSize);
    }

    private List<Integer> readDifat() throws IOException {
        List<Integer> sectors = new ArrayList<>();
        for (int i = 0; i < 109; i++) {
            int sid = i32(data, 76 + i * 4);
            if (sid >= 0) sectors.add(sid);
        }

        int next = firstDifatSector;
        for (int i = 0; i < difatSectorCount && next >= 0; i++) {
            byte[] sector = sectorBytes(next);
            int entries = sectorSize / 4 - 1;
            for (int j = 0; j < entries; j++) {
                int sid = i32(sector, j * 4);
                if (sid >= 0) sectors.add(sid);
            }
            next = i32(sector, entries * 4);
        }
        return sectors;
    }

    private int[] readFat() throws IOException {
        List<Integer> entries = new ArrayList<>();
        for (Integer sectorId : difat) {
            if (sectorId == null || sectorId < 0) continue;
            byte[] sector = sectorBytes(sectorId);
            for (int offset = 0; offset + 4 <= sector.length; offset += 4) {
                entries.add(i32(sector, offset));
            }
        }
        int[] out = new int[entries.size()];
        for (int i = 0; i < entries.size(); i++) out[i] = entries.get(i);
        return out;
    }

    private int[] readMiniFat() throws IOException {
        if (firstMiniFatSector < 0 || miniFatSectorCount <= 0) return new int[0];
        byte[] bytes = readRegularChain(firstMiniFatSector, miniFatSectorCount * sectorSize);
        int[] out = new int[bytes.length / 4];
        for (int i = 0; i < out.length; i++) out[i] = i32(bytes, i * 4);
        return out;
    }

    private byte[] readRegularChain(int startSector, int size) throws IOException {
        if (startSector < 0) return new byte[0];
        byte[] out = new byte[size >= 0 ? Math.max(0, size) : 0];
        int written = 0;
        List<byte[]> chunks = size < 0 ? new ArrayList<byte[]>() : null;
        int sector = startSector;
        int guard = 0;
        while (sector >= 0 && sector < fat.length && guard++ <= fat.length) {
            byte[] chunk = sectorBytes(sector);
            if (size >= 0) {
                int n = Math.min(chunk.length, out.length - written);
                if (n > 0) {
                    System.arraycopy(chunk, 0, out, written, n);
                    written += n;
                }
                if (written >= out.length) break;
            } else {
                chunks.add(chunk);
            }
            int next = fat[sector];
            if (next == ENDOFCHAIN || next == FREESECT || next == FATSECT || next == DIFSECT) break;
            sector = next;
        }
        if (size >= 0) return out;
        out = new byte[chunks.size() * sectorSize];
        for (int i = 0; i < chunks.size(); i++) {
            System.arraycopy(chunks.get(i), 0, out, i * sectorSize, sectorSize);
        }
        return out;
    }

    private byte[] readMiniChain(int startSector, int size) throws IOException {
        byte[] out = new byte[Math.max(0, size)];
        int written = 0;
        int sector = startSector;
        int guard = 0;
        while (sector >= 0 && sector < miniFat.length && guard++ <= miniFat.length && written < out.length) {
            int offset = sector * miniSectorSize;
            if (offset < 0 || offset >= miniStream.length) {
                throw new IOException("Invalid mini stream sector");
            }
            int n = Math.min(miniSectorSize, Math.min(out.length - written, miniStream.length - offset));
            System.arraycopy(miniStream, offset, out, written, n);
            written += n;
            int next = miniFat[sector];
            if (next == ENDOFCHAIN || next == FREESECT) break;
            sector = next;
        }
        return out;
    }

    private byte[] sectorBytes(int sector) throws IOException {
        int offset = 512 + sector * sectorSize;
        if (sector < 0 || offset < 512 || offset + sectorSize > data.length) {
            throw new IOException("Invalid OLE2 sector: " + sector);
        }
        return Arrays.copyOfRange(data, offset, offset + sectorSize);
    }

    private static List<DirectoryEntry> parseDirectory(byte[] bytes) {
        List<DirectoryEntry> entries = new ArrayList<>();
        for (int offset = 0; offset + 128 <= bytes.length; offset += 128) {
            int type = bytes[offset + 66] & 0xFF;
            if (type == 0) continue;
            int nameBytes = u16(bytes, offset + 64);
            String name = "";
            if (nameBytes >= 2) {
                int chars = Math.min(nameBytes - 2, 64) / 2;
                StringBuilder sb = new StringBuilder(chars);
                for (int i = 0; i < chars; i++) {
                    char c = (char) u16(bytes, offset + i * 2);
                    if (c == 0) break;
                    sb.append(c);
                }
                name = sb.toString();
            }
            entries.add(new DirectoryEntry(name, type, i32(bytes, offset + 116), u64(bytes, offset + 120)));
        }
        return entries;
    }

    private static DirectoryEntry findRoot(List<DirectoryEntry> entries) {
        for (DirectoryEntry e : entries) {
            if (e.type == 5) return e;
        }
        return null;
    }

    private static boolean hasSignature(byte[] data) {
        for (int i = 0; i < SIGNATURE.length; i++) {
            if (data[i] != SIGNATURE[i]) return false;
        }
        return true;
    }

    static int u16(byte[] bytes, int offset) {
        return (bytes[offset] & 0xFF) | ((bytes[offset + 1] & 0xFF) << 8);
    }

    static int i32(byte[] bytes, int offset) {
        return (bytes[offset] & 0xFF)
                | ((bytes[offset + 1] & 0xFF) << 8)
                | ((bytes[offset + 2] & 0xFF) << 16)
                | (bytes[offset + 3] << 24);
    }

    static long u32(byte[] bytes, int offset) {
        return i32(bytes, offset) & 0xFFFFFFFFL;
    }

    static long u64(byte[] bytes, int offset) {
        return u32(bytes, offset) | (u32(bytes, offset + 4) << 32);
    }

    private static final class DirectoryEntry {
        final String name;
        final int type;
        final int startSector;
        final long streamSize;

        DirectoryEntry(String name, int type, int startSector, long streamSize) {
            this.name = name;
            this.type = type;
            this.startSector = startSector;
            this.streamSize = streamSize;
        }
    }
}
