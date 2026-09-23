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
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public final class MiniKafka {
    record ProduceResult(int partition, long offset) {}

    private static final AtomicInteger CORRELATION_IDS = new AtomicInteger();
    private static final ConcurrentMap<String, AtomicInteger> ROUND_ROBINS =
            new ConcurrentHashMap<>();

    public static void main(String[] args) throws Exception {
        if (args.length == 1 && args[0].equals("self-test")) {
            selfTest();
            return;
        }
        if (args.length < 1) usage();

        switch (args[0]) {
            case "broker" -> runBroker(args);
            case "create-topic" -> createTopicCommand(args);
            case "describe-topic" -> describeTopicCommand(args);
            case "produce" -> produceCommand(args);
            case "consume" -> consumeCommand(args);
            default -> usage();
        }
    }

    private static void runBroker(String[] args) throws IOException {
        if (args.length < 2 || args.length > 4) usage();
        int port = args.length >= 3 ? parsePort(args[2]) : 9092;
        long segmentBytes = args.length == 4 ? Long.parseLong(args[3])
                : PartitionLog.DEFAULT_SEGMENT_BYTES;
        try (Broker broker = new Broker(Path.of(args[1]), port, segmentBytes)) {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> closeQuietly(broker)));
            System.out.println("broker listening on port " + broker.port());
            broker.serve();
        }
    }

    private static void createTopicCommand(String[] args) throws IOException {
        if (args.length != 5) usage();
        int partitions = Integer.parseInt(args[4]);
        createTopic(args[1], parsePort(args[2]), args[3], partitions);
        System.out.printf("created topic %s with %d partitions%n", args[3], partitions);
    }

    private static void describeTopicCommand(String[] args) throws IOException {
        if (args.length != 4) usage();
        int partitions = partitionCount(args[1], parsePort(args[2]), args[3]);
        System.out.printf("topic=%s partitions=%d%n", args[3], partitions);
    }

    private static void produceCommand(String[] args) throws IOException {
        if (args.length < 6 || args.length > 7) usage();
        byte[] key = args[4].equals("-") ? null : utf8(args[4]);
        Integer partition = args.length == 7 ? parsePartition(args[6]) : null;
        ProduceResult result =
                produce(args[1], parsePort(args[2]), args[3], key, utf8(args[5]), partition);
        System.out.printf("partition=%d offset=%d%n", result.partition(), result.offset());
    }

    private static void consumeCommand(String[] args) throws IOException {
        if (args.length != 6) usage();
        int partition = parsePartition(args[4]);
        List<PartitionLog.Record> records =
                fetch(args[1], parsePort(args[2]), args[3], partition, Long.parseLong(args[5]));
        for (PartitionLog.Record record : records) {
            String key = record.key() == null ? "-" : new String(record.key(), StandardCharsets.UTF_8);
            System.out.printf("%d\t%s\t%s%n", record.offset(), key,
                    new String(record.value(), StandardCharsets.UTF_8));
        }
    }

    static void createTopic(String host, int port, String topic, int partitions) throws IOException {
        int correlationId = CORRELATION_IDS.incrementAndGet();
        ByteArrayOutputStream bytes = request(correlationId, Protocol.CREATE_TOPIC);
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            Protocol.writeString(output, topic);
            output.writeInt(partitions);
        }
        try (DataInputStream response = exchange(host, port, bytes.toByteArray(), correlationId)) {
            Protocol.requireEnd(response);
        }
    }

    static int partitionCount(String host, int port, String topic) throws IOException {
        int correlationId = CORRELATION_IDS.incrementAndGet();
        ByteArrayOutputStream bytes = request(correlationId, Protocol.METADATA);
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            Protocol.writeString(output, topic);
        }
        try (DataInputStream response = exchange(host, port, bytes.toByteArray(), correlationId)) {
            int partitions = response.readInt();
            if (partitions < 1) throw new Protocol.ProtocolException("invalid partition count");
            Protocol.requireEnd(response);
            return partitions;
        }
    }

    static ProduceResult produce(
            String host, int port, String topic, byte[] key, byte[] value, Integer explicitPartition)
            throws IOException {
        int partition = explicitPartition != null
                ? explicitPartition
                : choosePartition(host, port, topic, key);
        int correlationId = CORRELATION_IDS.incrementAndGet();
        ByteArrayOutputStream bytes = request(correlationId, Protocol.PRODUCE);
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            Protocol.writeString(output, topic);
            output.writeInt(partition);
            Protocol.writeNullableBytes(output, key);
            Protocol.writeBytes(output, value);
        }

        try (DataInputStream response = exchange(host, port, bytes.toByteArray(), correlationId)) {
            int storedPartition = response.readInt();
            long offset = response.readLong();
            if (storedPartition != partition || offset < 0) {
                throw new Protocol.ProtocolException("invalid produce response");
            }
            Protocol.requireEnd(response);
            return new ProduceResult(storedPartition, offset);
        }
    }

    static List<PartitionLog.Record> fetch(
            String host, int port, String topic, int partition, long offset) throws IOException {
        int correlationId = CORRELATION_IDS.incrementAndGet();
        ByteArrayOutputStream bytes = request(correlationId, Protocol.FETCH);
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            Protocol.writeString(output, topic);
            output.writeInt(partition);
            output.writeLong(offset);
        }

        try (DataInputStream response = exchange(host, port, bytes.toByteArray(), correlationId)) {
            int count = response.readInt();
            int minimumRecordSize = Long.BYTES * 2 + Integer.BYTES * 2;
            if (count < 0 || count > response.available() / minimumRecordSize) {
                throw new Protocol.ProtocolException("invalid record count");
            }

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

    private static int choosePartition(String host, int port, String topic, byte[] key)
            throws IOException {
        int partitions = partitionCount(host, port, topic);
        if (key != null) return Math.floorMod(Arrays.hashCode(key), partitions);

        String brokerTopic = host + "\0" + port + "\0" + topic;
        return Math.floorMod(
                ROUND_ROBINS.computeIfAbsent(brokerTopic, ignored -> new AtomicInteger())
                        .getAndIncrement(),
                partitions);
    }

    private static ByteArrayOutputStream request(int correlationId, byte apiKey) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream output = new DataOutputStream(bytes);
        output.writeInt(correlationId);
        output.writeByte(apiKey);
        return bytes;
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

    private static int parsePartition(String value) {
        int partition = Integer.parseInt(value);
        if (partition < 0) throw new IllegalArgumentException("partition must be >= 0");
        return partition;
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
            String host = "127.0.0.1";
            int port = broker.port();

            createTopic(host, port, "orders", 3);
            assert partitionCount(host, port, "orders") == 3;

            ProduceResult partitionZero =
                    produce(host, port, "orders", utf8("explicit-0"), utf8("zero"), 0);
            ProduceResult partitionOne =
                    produce(host, port, "orders", utf8("explicit-1"), utf8("one"), 1);
            assert partitionZero.offset() == 0;
            assert partitionOne.offset() == 0;

            ProduceResult sameKeyFirst =
                    produce(host, port, "orders", utf8("alice"), utf8("coffee"), null);
            ProduceResult sameKeySecond =
                    produce(host, port, "orders", utf8("alice"), utf8("tea"), null);
            assert sameKeyFirst.partition() == sameKeySecond.partition();
            assert sameKeySecond.offset() == sameKeyFirst.offset() + 1;

            Set<Integer> roundRobinPartitions = new HashSet<>();
            for (int index = 0; index < 3; index++) {
                roundRobinPartitions.add(
                        produce(host, port, "orders", null, utf8("anonymous-" + index), null)
                                .partition());
            }
            assert roundRobinPartitions.equals(Set.of(0, 1, 2));

            List<PartitionLog.Record> records = fetch(host, port, "orders", 1, 0);
            assert records.get(0).offset() == 0;
            assert new String(records.get(0).value(), StandardCharsets.UTF_8).equals("one");

            rejectOversizedFrame(port);
            broker.close();
            brokerThread.join(2_000);
            assert !brokerThread.isAlive();
            if (brokerFailure.get() != null) throw new AssertionError(brokerFailure.get());

            assert Files.readString(directory.resolve("orders").resolve("topic.meta"))
                    .trim().equals("3");
            assert Files.isRegularFile(directory.resolve("orders/0/0.log"));
            assert Files.isRegularFile(directory.resolve("orders/1/0.log"));
            assert Files.isRegularFile(directory.resolve("orders/2/0.log"));

            AtomicReference<Throwable> restartFailure = new AtomicReference<>();
            Thread restartedThread;
            try (Broker restarted = new Broker(directory, 0)) {
                restartedThread = new Thread(() -> {
                    try {
                        restarted.serve();
                    } catch (Throwable failure) {
                        restartFailure.set(failure);
                    }
                }, "mini-kafka-restart-test");
                restartedThread.start();
                assert partitionCount(host, restarted.port(), "orders") == 3;
            }
            restartedThread.join(2_000);
            assert !restartedThread.isAlive();
            if (restartFailure.get() != null) throw new AssertionError(restartFailure.get());
            segmentSelfTest(directory);
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

    private static void segmentSelfTest(Path directory) throws IOException {
        Path legacyPath = directory.resolve("segments.log");
        Path segmentDirectory = directory.resolve("segments");
        try (PartitionLog log = new PartitionLog(legacyPath, 1_400)) {
            for (int offset = 0; offset < 80; offset++) {
                assert log.append(null, utf8("message-" + offset + "-" + "x".repeat(30))) == offset;
            }
            List<PartitionLog.Record> records = log.readFrom(25);
            assert records.size() == 55;
            assert records.get(0).offset() == 25;
            assert records.get(54).offset() == 79;
            assert log.readFrom(17).get(0).offset() == 17;
            assert log.readFrom(79).get(0).offset() == 79;
            assert log.readFrom(25, 300).size() == 4;
        }

        List<Path> segmentFiles;
        try (var files = Files.list(segmentDirectory)) {
            segmentFiles = files.filter(path -> path.toString().endsWith(".log"))
                    .sorted(Comparator.comparingLong(path -> Long.parseLong(
                            path.getFileName().toString().replace(".log", "")))).toList();
        }
        assert segmentFiles.size() > 1;
        Path firstIndex = segmentDirectory.resolve("0.index");
        assert Files.size(firstIndex) >= 32;
        Files.write(firstIndex, new byte[] {1, 2, 3});

        Path activeSegment = segmentFiles.get(segmentFiles.size() - 1);
        long completeLength = Files.size(activeSegment);
        Files.write(activeSegment, new byte[] {0, 1}, java.nio.file.StandardOpenOption.APPEND);
        try (PartitionLog reopened = new PartitionLog(legacyPath, 1_400)) {
            assert Files.size(firstIndex) >= 32;
            assert Files.size(activeSegment) == completeLength;
            assert reopened.readFrom(25).size() == 55;
            assert reopened.append(null, utf8("after restart")) == 80;
        }

        Files.write(segmentFiles.get(0), new byte[] {0}, java.nio.file.StandardOpenOption.APPEND);
        try {
            new PartitionLog(legacyPath, 1_400).close();
            throw new AssertionError("incomplete closed segment was accepted");
        } catch (IOException expected) {
            assert expected.getMessage().contains("incomplete closed segment");
        }

        Path originalPath = directory.resolve("legacy.log");
        Path originalDirectory = directory.resolve("legacy");
        try (PartitionLog old = new PartitionLog(originalPath)) {
            assert old.append(null, utf8("old data")) == 0;
        }
        Files.move(originalDirectory.resolve("0.log"), originalPath);
        Files.delete(originalDirectory.resolve("0.index"));
        Files.delete(originalDirectory);
        try (PartitionLog migrated = new PartitionLog(originalPath)) {
            assert migrated.readFrom(0).size() == 1;
            assert migrated.append(null, utf8("new data")) == 1;
        }
        assert Files.isRegularFile(originalDirectory.resolve("0.log"));
        assert !Files.exists(originalPath);
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
        System.err.println("  MiniKafka broker <data-dir> [port] [segment-bytes]");
        System.err.println("  MiniKafka create-topic <host> <port> <topic> <partitions>");
        System.err.println("  MiniKafka describe-topic <host> <port> <topic>");
        System.err.println("  MiniKafka produce <host> <port> <topic> <key|-> <value> [partition]");
        System.err.println("  MiniKafka consume <host> <port> <topic> <partition> <offset>");
        System.err.println("  MiniKafka self-test");
        System.exit(2);
    }
}
