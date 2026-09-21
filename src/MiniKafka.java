import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public final class MiniKafka {
    private static final AtomicInteger CORRELATION_IDS = new AtomicInteger();

    public static void main(String[] args) throws Exception {
        if (args.length == 1 && args[0].equals("self-test")) {
            selfTest();
            return;
        }
        if (args.length < 1) usage();

        switch (args[0]) {
            case "broker" -> runBroker(args);
            case "produce" -> produceCommand(args);
            case "consume" -> consumeCommand(args);
            default -> usage();
        }
    }

    private static void runBroker(String[] args) throws IOException {
        if (args.length < 2 || args.length > 3) usage();
        int port = args.length == 3 ? parsePort(args[2]) : 9092;
        try (Broker broker = new Broker(Path.of(args[1]), port)) {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> closeQuietly(broker)));
            System.out.println("broker listening on port " + broker.port());
            broker.serve();
        }
    }

    private static void produceCommand(String[] args) throws IOException {
        if (args.length != 6) usage();
        byte[] key = args[4].equals("-") ? null : utf8(args[4]);
        long offset = produce(args[1], parsePort(args[2]), args[3], key, utf8(args[5]));
        System.out.println(offset);
    }

    private static void consumeCommand(String[] args) throws IOException {
        if (args.length != 5) usage();
        List<PartitionLog.Record> records =
                fetch(args[1], parsePort(args[2]), args[3], Long.parseLong(args[4]));
        for (PartitionLog.Record record : records) {
            String key = record.key() == null ? "-" : new String(record.key(), StandardCharsets.UTF_8);
            System.out.printf("%d\t%s\t%s%n", record.offset(), key,
                    new String(record.value(), StandardCharsets.UTF_8));
        }
    }

    static long produce(String host, int port, String topic, byte[] key, byte[] value)
            throws IOException {
        int correlationId = CORRELATION_IDS.incrementAndGet();
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            output.writeInt(correlationId);
            output.writeByte(Protocol.PRODUCE);
            Protocol.writeString(output, topic);
            Protocol.writeNullableBytes(output, key);
            Protocol.writeBytes(output, value);
        }

        try (DataInputStream response = exchange(host, port, bytes.toByteArray(), correlationId)) {
            long offset = response.readLong();
            Protocol.requireEnd(response);
            return offset;
        }
    }

    static List<PartitionLog.Record> fetch(String host, int port, String topic, long offset)
            throws IOException {
        int correlationId = CORRELATION_IDS.incrementAndGet();
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            output.writeInt(correlationId);
            output.writeByte(Protocol.FETCH);
            Protocol.writeString(output, topic);
            output.writeLong(offset);
        }

        try (DataInputStream response = exchange(host, port, bytes.toByteArray(), correlationId)) {
            int count = response.readInt();
            if (count < 0) throw new Protocol.ProtocolException("negative record count");

            List<PartitionLog.Record> records = new ArrayList<>(count);
            for (int index = 0; index < count; index++) {
                long recordOffset = response.readLong();
                long timestamp = response.readLong();
                byte[] key = Protocol.readBytes(response, true);
                byte[] value = Protocol.readBytes(response, false);
                records.add(new PartitionLog.Record(recordOffset, timestamp, key, value));
            }
            Protocol.requireEnd(response);
            return records;
        }
    }

    private static DataInputStream exchange(
            String host, int port, byte[] request, int correlationId) throws IOException {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), 3_000);
            socket.setSoTimeout(5_000);
            DataOutputStream output = new DataOutputStream(socket.getOutputStream());
            Protocol.writeFrame(output, request);

            byte[] responseBytes = Protocol.readFrame(new DataInputStream(socket.getInputStream()));
            DataInputStream response = new DataInputStream(new ByteArrayInputStream(responseBytes));
            int responseCorrelationId = response.readInt();
            if (responseCorrelationId != correlationId) {
                throw new Protocol.ProtocolException("correlation id mismatch");
            }
            byte status = response.readByte();
            if (status != Protocol.OK) throw new IOException(Protocol.readString(response));
            return response;
        }
    }

    private static int parsePort(String value) {
        int port = Integer.parseInt(value);
        if (port < 1 || port > 65_535) throw new IllegalArgumentException("invalid port");
        return port;
    }

    private static byte[] utf8(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static void selfTest() throws Exception {
        Path directory = Files.createTempDirectory("mini-kafka-");
        AtomicReference<Throwable> brokerFailure = new AtomicReference<>();
        Broker broker = new Broker(directory, 0);
        Thread brokerThread = new Thread(() -> {
            try {
                broker.serve();
            } catch (Throwable failure) {
                brokerFailure.set(failure);
            }
        }, "mini-kafka-self-test");
        try {
            brokerThread.start();

            assert produce("127.0.0.1", broker.port(), "orders", utf8("alice"), utf8("coffee")) == 0;
            assert produce("127.0.0.1", broker.port(), "orders", null, utf8("tea")) == 1;
            List<PartitionLog.Record> records = fetch("127.0.0.1", broker.port(), "orders", 1);
            assert records.size() == 1;
            assert records.get(0).offset() == 1;
            assert new String(records.get(0).value(), StandardCharsets.UTF_8).equals("tea");

            rejectOversizedFrame(broker.port());
            broker.close();
            brokerThread.join(2_000);
            assert !brokerThread.isAlive();
            if (brokerFailure.get() != null) throw new AssertionError(brokerFailure.get());

            try (PartitionLog reopened =
                         new PartitionLog(directory.resolve("orders").resolve("0.log"))) {
                assert reopened.append(null, utf8("water")) == 2;
            }
            System.out.println("self-test passed");
        } finally {
            broker.close();
            brokerThread.join(2_000);
            try (var paths = Files.walk(directory)) {
                paths.sorted(Comparator.reverseOrder()).forEach(path -> {
                    try {
                        Files.delete(path);
                    } catch (IOException error) {
                        throw new RuntimeException(error);
                    }
                });
            }
        }
    }

    private static void rejectOversizedFrame(int port) throws IOException {
        try (Socket socket = new Socket("127.0.0.1", port);
             DataOutputStream output = new DataOutputStream(socket.getOutputStream())) {
            output.writeInt(Protocol.MAX_FRAME_SIZE + 1);
            output.flush();
            assert socket.getInputStream().read() == -1;
        }
    }

    private static void closeQuietly(Broker broker) {
        try {
            broker.close();
        } catch (IOException ignored) {
        }
    }

    private static void usage() {
        System.err.println("Usage:");
        System.err.println("  MiniKafka broker <data-dir> [port]");
        System.err.println("  MiniKafka produce <host> <port> <topic> <key|-> <value>");
        System.err.println("  MiniKafka consume <host> <port> <topic> <offset>");
        System.err.println("  MiniKafka self-test");
        System.exit(2);
    }
}
