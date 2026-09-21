import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.zip.CRC32;

public final class MiniKafka {
    private static final int MIN_PAYLOAD_SIZE = Long.BYTES * 2 + Integer.BYTES * 2;
    private static final int MAX_PAYLOAD_SIZE = 16 * 1024 * 1024;

    record Record(long offset, long timestamp, byte[] key, byte[] value) {}

    static final class Log implements AutoCloseable {
        private final RandomAccessFile file;
        private long nextOffset;

        Log(Path path) throws IOException {
            Files.createDirectories(path.getParent());
            file = new RandomAccessFile(path.toFile(), "rw");
            recover();
        }

        synchronized long append(byte[] key, byte[] value) throws IOException {
            if (value == null) throw new IllegalArgumentException("value must not be null");

            long offset = nextOffset;
            byte[] payload = encode(offset, System.currentTimeMillis(), key, value);
            CRC32 crc = new CRC32();
            crc.update(payload);

            file.seek(file.length());
            file.writeInt(payload.length);
            file.write(payload);
            file.writeInt((int) crc.getValue());
            file.getFD().sync();
            nextOffset++;
            return offset;
        }

        synchronized List<Record> readFrom(long requestedOffset) throws IOException {
            if (requestedOffset < 0) throw new IllegalArgumentException("offset must be >= 0");

            file.seek(0);
            List<Record> records = new ArrayList<>();
            while (file.getFilePointer() < file.length()) {
                Record record = readRecord();
                if (record.offset() >= requestedOffset) records.add(record);
            }
            return records;
        }

        private void recover() throws IOException {
            file.seek(0);
            long lastGoodPosition = 0;
            long expectedOffset = 0;

            while (file.getFilePointer() < file.length()) {
                try {
                    Record record = readRecord();
                    if (record.offset() != expectedOffset) {
                        throw new IOException("non-sequential offset " + record.offset()
                                + ", expected " + expectedOffset);
                    }
                    expectedOffset++;
                    lastGoodPosition = file.getFilePointer();
                } catch (EOFException incompleteTail) {
                    file.setLength(lastGoodPosition);
                    break;
                }
            }
            nextOffset = expectedOffset;
        }

        private Record readRecord() throws IOException {
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
                if (keySize < -1 || valueSize < 0 || (long) Math.max(keySize, 0) + valueSize != input.available()) {
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
        public void close() throws IOException {
            file.close();
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 1 && args[0].equals("self-test")) {
            selfTest();
            return;
        }
        if (args.length < 1) usage();

        switch (args[0]) {
            case "produce" -> produce(args);
            case "consume" -> consume(args);
            default -> usage();
        }
    }

    private static void produce(String[] args) throws IOException {
        if (args.length != 5) usage();
        Path path = logPath(args[1], args[2]);
        byte[] key = args[3].equals("-") ? null : utf8(args[3]);
        try (Log log = new Log(path)) {
            System.out.println(log.append(key, utf8(args[4])));
        }
    }

    private static void consume(String[] args) throws IOException {
        if (args.length != 4) usage();
        long offset = Long.parseLong(args[3]);
        try (Log log = new Log(logPath(args[1], args[2]))) {
            for (Record record : log.readFrom(offset)) {
                String key = record.key() == null ? "-" : new String(record.key(), StandardCharsets.UTF_8);
                System.out.printf("%d\t%s\t%s%n", record.offset(), key,
                        new String(record.value(), StandardCharsets.UTF_8));
            }
        }
    }

    private static Path logPath(String dataDirectory, String topic) {
        if (!topic.matches("[A-Za-z0-9._-]+")) throw new IllegalArgumentException("invalid topic name");
        return Path.of(dataDirectory, topic, "0.log");
    }

    private static byte[] utf8(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static void selfTest() throws Exception {
        Path directory = Files.createTempDirectory("mini-kafka-");
        Path path = logPath(directory.toString(), "orders");
        try {
            try (Log log = new Log(path)) {
                assert log.append(utf8("alice"), utf8("coffee")) == 0;
                assert log.append(null, utf8("tea")) == 1;
            }
            try (Log reopened = new Log(path)) {
                List<Record> records = reopened.readFrom(1);
                assert records.size() == 1;
                assert records.get(0).offset() == 1;
                assert new String(records.get(0).value(), StandardCharsets.UTF_8).equals("tea");
                assert reopened.append(null, utf8("water")) == 2;
            }
            System.out.println("self-test passed");
        } finally {
            try (var paths = Files.walk(directory)) {
                paths.sorted(Comparator.reverseOrder()).forEach(pathToDelete -> {
                    try {
                        Files.delete(pathToDelete);
                    } catch (IOException error) {
                        throw new RuntimeException(error);
                    }
                });
            }
        }
    }

    private static void usage() {
        System.err.println("Usage:");
        System.err.println("  MiniKafka produce <data-dir> <topic> <key|-> <value>");
        System.err.println("  MiniKafka consume <data-dir> <topic> <offset>");
        System.err.println("  MiniKafka self-test");
        System.exit(2);
    }
}
