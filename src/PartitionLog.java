import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.zip.CRC32;

final class PartitionLog implements AutoCloseable {
    private static final int MIN_PAYLOAD_SIZE = Long.BYTES * 2 + Integer.BYTES * 2;
    private static final int MAX_PAYLOAD_SIZE = 16 * 1024 * 1024;
    static final long DEFAULT_SEGMENT_BYTES = 1024 * 1024;
    private static final int INDEX_INTERVAL = 16;
    private static final int INDEX_ENTRY_BYTES = Long.BYTES * 2;

    record Record(long offset, long timestamp, byte[] key, byte[] value) {}

    private final Path directory;
    private final long segmentBytes;
    // ponytail: one open handle pair per segment; close inactive segments when this becomes a limit.
    private final List<Segment> segments = new ArrayList<>();
    private long nextOffset;

    PartitionLog(Path legacyPath) throws IOException {
        this(legacyPath, DEFAULT_SEGMENT_BYTES);
    }

    PartitionLog(Path legacyPath, long segmentBytes) throws IOException {
        if (segmentBytes < 1) throw new IllegalArgumentException("segment size must be positive");
        this.segmentBytes = segmentBytes;
        String fileName = legacyPath.getFileName().toString();
        if (!fileName.endsWith(".log")) throw new IllegalArgumentException("expected .log path");
        directory = legacyPath.resolveSibling(fileName.substring(0, fileName.length() - 4));

        if (Files.exists(legacyPath)) {
            if (Files.exists(directory)) throw new IOException("legacy and segmented logs both exist");
            Files.createDirectories(directory);
            Files.move(legacyPath, directory.resolve("0.log"));
        } else {
            Files.createDirectories(directory);
        }
        try {
            recover();
        } catch (IOException | RuntimeException failure) {
            try {
                close();
            } catch (IOException closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            throw failure;
        }
    }

    synchronized long append(byte[] key, byte[] value) throws IOException {
        if (value == null) throw new IllegalArgumentException("value must not be null");
        byte[] payload = encode(nextOffset, System.currentTimeMillis(), key, value);
        Segment active = segments.get(segments.size() - 1);
        if (active.log.length() >= segmentBytes && active.log.length() > 0) {
            active = openSegment(nextOffset);
            segments.add(active);
        }

        long offset = nextOffset;
        CRC32 crc = new CRC32();
        crc.update(payload);

        long position = active.log.length();
        active.log.seek(position);
        active.log.writeInt(payload.length);
        active.log.write(payload);
        active.log.writeInt((int) crc.getValue());
        active.log.getFD().sync();
        boolean indexThisRecord = active.records % INDEX_INTERVAL == 0;
        active.records++;
        nextOffset++;
        if (indexThisRecord) active.addIndex(offset, position);
        return offset;
    }

    synchronized List<Record> readFrom(long requestedOffset) throws IOException {
        return readFrom(requestedOffset, Long.MAX_VALUE);
    }

    synchronized List<Record> readFrom(long requestedOffset, long maxResponseBytes) throws IOException {
        if (requestedOffset < 0) throw new IllegalArgumentException("offset must be >= 0");
        List<Record> records = new ArrayList<>();
        if (requestedOffset >= nextOffset) return records;

        int firstSegment = segmentFor(requestedOffset);
        for (int i = firstSegment; i < segments.size(); i++) {
            Segment segment = segments.get(i);
            segment.log.seek(segment.positionFor(requestedOffset));
            while (segment.log.getFilePointer() < segment.log.length()) {
                Record record = readRecord(segment.log);
                if (record.offset() >= requestedOffset) {
                    long recordBytes = 24L + (record.key() == null ? 0 : record.key().length)
                            + record.value().length;
                    if (recordBytes > maxResponseBytes) {
                        if (records.isEmpty()) throw new IOException("record exceeds fetch frame limit");
                        return records;
                    }
                    records.add(record);
                    maxResponseBytes -= recordBytes;
                }
            }
        }
        return records;
    }

    private int segmentFor(long offset) {
        int low = 0;
        int high = segments.size();
        while (low + 1 < high) {
            int middle = (low + high) >>> 1;
            if (segments.get(middle).baseOffset <= offset) low = middle;
            else high = middle;
        }
        return low;
    }

    private void recover() throws IOException {
        List<Long> bases = new ArrayList<>();
        try (var files = Files.list(directory)) {
            for (Path path : files.toList()) {
                String name = path.getFileName().toString();
                if (!name.matches("[0-9]+\\.log")) continue;
                try {
                    long base = Long.parseLong(name.substring(0, name.length() - 4));
                    if (!name.equals(base + ".log")) {
                        throw new IOException("invalid segment name: " + name);
                    }
                    bases.add(base);
                } catch (NumberFormatException invalid) {
                    throw new IOException("invalid segment name: " + name, invalid);
                }
            }
        }
        bases.sort(Comparator.naturalOrder());
        if (bases.isEmpty()) bases.add(0L);

        long expectedOffset = 0;
        for (int i = 0; i < bases.size(); i++) {
            long base = bases.get(i);
            if (base != expectedOffset) throw new IOException("unexpected segment base offset: " + base);
            Segment segment = openSegment(base);
            segments.add(segment);
            segment.log.seek(0);
            long lastGoodPosition = 0;
            while (segment.log.getFilePointer() < segment.log.length()) {
                long position = segment.log.getFilePointer();
                try {
                    Record record = readRecord(segment.log);
                    if (record.offset() != expectedOffset) {
                        throw new IOException("non-sequential offset " + record.offset()
                                + ", expected " + expectedOffset);
                    }
                    if (segment.records % INDEX_INTERVAL == 0) {
                        segment.addIndex(expectedOffset, position);
                    }
                    expectedOffset++;
                    segment.records++;
                    lastGoodPosition = segment.log.getFilePointer();
                } catch (EOFException incompleteTail) {
                    if (i != bases.size() - 1) {
                        throw new IOException("incomplete closed segment: " + base, incompleteTail);
                    }
                    segment.log.setLength(lastGoodPosition);
                    break;
                }
            }
            segment.index.getFD().sync();
        }
        nextOffset = expectedOffset;
    }

    private Segment openSegment(long baseOffset) throws IOException {
        return new Segment(baseOffset, directory.resolve(baseOffset + ".log"),
                directory.resolve(baseOffset + ".index"));
    }

    private static Record readRecord(RandomAccessFile file) throws IOException {
        int payloadSize = file.readInt();
        if (payloadSize < MIN_PAYLOAD_SIZE || payloadSize > MAX_PAYLOAD_SIZE) {
            throw new IOException("invalid record size: " + payloadSize);
        }

        byte[] payload = new byte[payloadSize];
        file.readFully(payload);
        int storedCrc = file.readInt();
        CRC32 crc = new CRC32();
        crc.update(payload);
        if (storedCrc != (int) crc.getValue()) throw new IOException("record checksum mismatch");

        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(payload))) {
            long offset = input.readLong();
            long timestamp = input.readLong();
            int keySize = input.readInt();
            int valueSize = input.readInt();
            if (keySize < -1 || valueSize < 0
                    || (long) Math.max(keySize, 0) + valueSize != input.available()) {
                throw new IOException("invalid key/value sizes");
            }
            byte[] key = keySize == -1 ? null : input.readNBytes(keySize);
            byte[] value = input.readNBytes(valueSize);
            return new Record(offset, timestamp, key, value);
        }
    }

    private static byte[] encode(long offset, long timestamp, byte[] key, byte[] value)
            throws IOException {
        int keySize = key == null ? -1 : key.length;
        long payloadSize = (long) MIN_PAYLOAD_SIZE + Math.max(keySize, 0) + value.length;
        if (payloadSize > MAX_PAYLOAD_SIZE) throw new IllegalArgumentException("record is too large");

        ByteArrayOutputStream bytes = new ByteArrayOutputStream((int) payloadSize);
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            output.writeLong(offset);
            output.writeLong(timestamp);
            output.writeInt(keySize);
            output.writeInt(value.length);
            if (key != null) output.write(key);
            output.write(value);
        }
        return bytes.toByteArray();
    }

    @Override
    public synchronized void close() throws IOException {
        IOException failure = null;
        for (Segment segment : segments) {
            try {
                segment.close();
            } catch (IOException error) {
                if (failure == null) failure = error;
                else failure.addSuppressed(error);
            }
        }
        if (failure != null) throw failure;
    }

    private static final class Segment implements AutoCloseable {
        private final long baseOffset;
        private final RandomAccessFile log;
        private final RandomAccessFile index;
        private long records;

        Segment(long baseOffset, Path logPath, Path indexPath) throws IOException {
            this.baseOffset = baseOffset;
            log = new RandomAccessFile(logPath.toFile(), "rw");
            try {
                index = new RandomAccessFile(indexPath.toFile(), "rw");
                index.setLength(0);
            } catch (IOException failure) {
                log.close();
                throw failure;
            }
        }

        void addIndex(long offset, long position) throws IOException {
            index.seek(index.length());
            index.writeLong(offset);
            index.writeLong(position);
        }

        long positionFor(long offset) throws IOException {
            long count = index.length() / INDEX_ENTRY_BYTES;
            long low = 0;
            long high = count;
            while (low < high) {
                long middle = (low + high) >>> 1;
                index.seek(middle * INDEX_ENTRY_BYTES);
                if (index.readLong() <= offset) low = middle + 1;
                else high = middle;
            }
            if (low == 0) return 0;
            index.seek((low - 1) * INDEX_ENTRY_BYTES + Long.BYTES);
            return index.readLong();
        }

        @Override
        public void close() throws IOException {
            try {
                index.close();
            } finally {
                log.close();
            }
        }
    }
}
