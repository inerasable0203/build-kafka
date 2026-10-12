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
import java.nio.file.attribute.FileTime;
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
import java.util.concurrent.atomic.AtomicLong;

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
            case "delete" -> deleteCommand(args);
            case "consume" -> consumeCommand(args);
            case "consume-group" -> consumeGroupCommand(args);
            case "group-offset" -> groupOffsetCommand(args);
            case "compact" -> compactCommand(args);
            default -> usage();
        }
    }

    private static void runBroker(String[] args) throws IOException {
        if (args.length < 2 || args.length > 6) usage();
        int port = args.length >= 3 ? parsePort(args[2]) : 9092;
        long segmentBytes = args.length >= 4 ? Long.parseLong(args[3])
                : PartitionLog.DEFAULT_SEGMENT_BYTES;
        long retentionMs = args.length >= 5 ? Long.parseLong(args[4]) : -1;
        long retentionBytes = args.length == 6 ? Long.parseLong(args[5]) : -1;
        try (Broker broker = new Broker(Path.of(args[1]), port, segmentBytes,
                retentionMs, retentionBytes)) {
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

    private static void deleteCommand(String[] args) throws IOException {
        if (args.length < 5 || args.length > 6 || args[4].equals("-")) usage();
        Integer partition = args.length == 6 ? parsePartition(args[5]) : null;
        ProduceResult result = produce(args[1], parsePort(args[2]), args[3],
                utf8(args[4]), null, partition);
        System.out.printf("partition=%d offset=%d tombstone%n", result.partition(), result.offset());
    }

    private static void compactCommand(String[] args) throws IOException {
        if (args.length != 5) usage();
        compact(args[1], parsePort(args[2]), args[3], parsePartition(args[4]));
        System.out.println("compaction complete");
    }

    private static void consumeCommand(String[] args) throws IOException {
        if (args.length != 6) usage();
        int partition = parsePartition(args[4]);
        List<PartitionLog.Record> records =
                fetch(args[1], parsePort(args[2]), args[3], partition, Long.parseLong(args[5]));
        for (PartitionLog.Record record : records) {
            String key = record.key() == null ? "-" : new String(record.key(), StandardCharsets.UTF_8);
            String value = record.value() == null ? "<tombstone>"
                    : new String(record.value(), StandardCharsets.UTF_8);
            System.out.printf("%d\t%s\t%s%n", record.offset(), key, value);
        }
    }

    private static void groupOffsetCommand(String[] args) throws IOException {
        if (args.length != 6) usage();
        long offset = fetchCommittedOffset(args[1], parsePort(args[2]), args[3], args[4],
                parsePartition(args[5]));
        System.out.println(offset < 0 ? "no committed offset" : "next offset=" + offset);
    }

    private static void consumeGroupCommand(String[] args) throws Exception {
        if (args.length < 5 || args.length > 6) usage();
        String host = args[1];
        int port = parsePort(args[2]);
        String group = args[3];
        String topic = args[4];
        int polls = args.length == 6 ? Integer.parseInt(args[5]) : Integer.MAX_VALUE;
        if (polls < 1) throw new IllegalArgumentException("polls must be positive");

        ConsumerGroups.Assignment session = joinGroup(host, port, group, topic);
        String memberId = session.memberId();
        Thread shutdown = new Thread(() -> {
            try {
                leaveGroup(host, port, group, topic, memberId);
            } catch (IOException ignored) {
            }
        }, "leave-consumer-group");
        Runtime.getRuntime().addShutdownHook(shutdown);
        try {
            for (int poll = 0; poll < polls; poll++) {
                ConsumerGroups.Assignment updated = heartbeat(host, port, group, topic, memberId);
                if (session.generation() != updated.generation()
                        || !session.partitions().equals(updated.partitions())) {
                    System.out.printf("generation=%d partitions=%s%n",
                            updated.generation(), updated.partitions());
                } else if (poll == 0) {
                    System.out.printf("member=%s partitions=%s%n", memberId, updated.partitions());
                }
                session = updated;
                for (int partition : session.partitions()) {
                    long offset = fetchCommittedOffset(host, port, group, topic, partition);
                    List<PartitionLog.Record> records = fetch(host, port, topic, partition,
                            Math.max(0, offset));
                    for (PartitionLog.Record record : records) {
                        String key = record.key() == null ? "-"
                                : new String(record.key(), StandardCharsets.UTF_8);
                        String value = record.value() == null ? "<tombstone>"
                                : new String(record.value(), StandardCharsets.UTF_8);
                        System.out.printf("partition=%d offset=%d key=%s value=%s%n",
                                partition, record.offset(), key, value);
                    }
                    if (!records.isEmpty()) {
                        long nextOffset = records.get(records.size() - 1).offset() + 1;
                        try {
                            commitOffset(host, port, group, topic, memberId,
                                    session.generation(), partition, nextOffset);
                        } catch (IOException failure) {
                            if (!"stale generation or unassigned partition".equals(failure.getMessage())) {
                                throw failure;
                            }
                            break; // Rebalance raced with processing; retry from the last committed offset.
                        }
                    }
                }
                if (poll + 1 < polls) Thread.sleep(1_000);
            }
        } finally {
            try {
                Runtime.getRuntime().removeShutdownHook(shutdown);
                leaveGroup(host, port, group, topic, memberId);
            } catch (IllegalStateException shuttingDown) {
                // The shutdown hook will leave the group.
            } catch (IOException unavailable) {
                System.err.println("could not leave group: " + unavailable.getMessage());
            }
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
            Protocol.writeNullableBytes(output, value);
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
                byte[] value = Protocol.readBytes(response, true);
                records.add(new PartitionLog.Record(recordOffset, timestamp, key, value));
            }
            Protocol.requireEnd(response);
            return records;
        }
    }

    static void compact(String host, int port, String topic, int partition) throws IOException {
        int correlationId = CORRELATION_IDS.incrementAndGet();
        ByteArrayOutputStream bytes = request(correlationId, Protocol.COMPACT);
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            Protocol.writeString(output, topic);
            output.writeInt(partition);
        }
        try (DataInputStream response = exchange(host, port, bytes.toByteArray(), correlationId)) {
            Protocol.requireEnd(response);
        }
    }

    static ConsumerGroups.Assignment joinGroup(String host, int port, String group, String topic)
            throws IOException {
        int correlationId = CORRELATION_IDS.incrementAndGet();
        ByteArrayOutputStream bytes = request(correlationId, Protocol.JOIN_GROUP);
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            Protocol.writeString(output, group);
            Protocol.writeString(output, topic);
        }
        try (DataInputStream response = exchange(host, port, bytes.toByteArray(), correlationId)) {
            return readAssignment(response);
        }
    }

    static ConsumerGroups.Assignment heartbeat(String host, int port, String group,
            String topic, String memberId) throws IOException {
        int correlationId = CORRELATION_IDS.incrementAndGet();
        ByteArrayOutputStream bytes = request(correlationId, Protocol.HEARTBEAT);
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            Protocol.writeString(output, group);
            Protocol.writeString(output, topic);
            Protocol.writeString(output, memberId);
        }
        try (DataInputStream response = exchange(host, port, bytes.toByteArray(), correlationId)) {
            return readAssignment(response);
        }
    }

    static void leaveGroup(String host, int port, String group, String topic, String memberId)
            throws IOException {
        int correlationId = CORRELATION_IDS.incrementAndGet();
        ByteArrayOutputStream bytes = request(correlationId, Protocol.LEAVE_GROUP);
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            Protocol.writeString(output, group);
            Protocol.writeString(output, topic);
            Protocol.writeString(output, memberId);
        }
        try (DataInputStream response = exchange(host, port, bytes.toByteArray(), correlationId)) {
            Protocol.requireEnd(response);
        }
    }

    static void commitOffset(String host, int port, String group, String topic,
            String memberId, int generation, int partition, long nextOffset) throws IOException {
        int correlationId = CORRELATION_IDS.incrementAndGet();
        ByteArrayOutputStream bytes = request(correlationId, Protocol.COMMIT_OFFSET);
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            Protocol.writeString(output, group);
            Protocol.writeString(output, topic);
            Protocol.writeString(output, memberId);
            output.writeInt(generation);
            output.writeInt(partition);
            output.writeLong(nextOffset);
        }
        try (DataInputStream response = exchange(host, port, bytes.toByteArray(), correlationId)) {
            Protocol.requireEnd(response);
        }
    }

    static long fetchCommittedOffset(String host, int port, String group, String topic,
            int partition) throws IOException {
        int correlationId = CORRELATION_IDS.incrementAndGet();
        ByteArrayOutputStream bytes = request(correlationId, Protocol.FETCH_OFFSET);
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            Protocol.writeString(output, group);
            Protocol.writeString(output, topic);
            output.writeInt(partition);
        }
        try (DataInputStream response = exchange(host, port, bytes.toByteArray(), correlationId)) {
            long offset = response.readLong();
            if (offset < -1) throw new Protocol.ProtocolException("invalid committed offset");
            Protocol.requireEnd(response);
            return offset;
        }
    }

    private static ConsumerGroups.Assignment readAssignment(DataInputStream response)
            throws IOException {
        String memberId = Protocol.readString(response);
        int generation = response.readInt();
        int count = response.readInt();
        if (generation < 1 || count < 0 || count > 1_000 || count > response.available() / Integer.BYTES) {
            throw new Protocol.ProtocolException("invalid group assignment");
        }
        List<Integer> partitions = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            int partition = response.readInt();
            if (partition < 0 || partitions.contains(partition)) {
                throw new Protocol.ProtocolException("invalid assigned partition");
            }
            partitions.add(partition);
        }
        Protocol.requireEnd(response);
        return new ConsumerGroups.Assignment(memberId, generation, List.copyOf(partitions));
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

            ProduceResult tombstone = produce(host, port, "orders", utf8("removed"), null, 1);
            assert fetch(host, port, "orders", 1, tombstone.offset()).get(0).value() == null;
            compact(host, port, "orders", 1);

            consumerGroupSelfTest(host, port);

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
                assert fetchCommittedOffset(host, restarted.port(), "workers", "orders", 0) == 1;
                ConsumerGroups.Assignment rejoined = joinGroup(host, restarted.port(),
                        "workers", "orders");
                assert rejoined.partitions().equals(List.of(0, 1, 2));
                assert fetch(host, restarted.port(), "orders", 0, 1).get(0).offset() == 1;
                commitOffset(host, restarted.port(), "workers", "orders", rejoined.memberId(),
                        rejoined.generation(), 0, 2);
                assert fetchCommittedOffset(host, restarted.port(), "workers", "orders", 0) == 2;
                leaveGroup(host, restarted.port(), "workers", "orders", rejoined.memberId());
            }
            restartedThread.join(2_000);
            assert !restartedThread.isAlive();
            if (restartFailure.get() != null) throw new AssertionError(restartFailure.get());
            segmentSelfTest(directory);
            retentionCompactionSelfTest(directory);
            groupTimeoutSelfTest(directory);
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

    private static void consumerGroupSelfTest(String host, int port) throws IOException {
        assert produce(host, port, "orders", utf8("resume"), utf8("second"), 0).offset() >= 1;
        ConsumerGroups.Assignment first = joinGroup(host, port, "workers", "orders");
        assert first.partitions().equals(List.of(0, 1, 2));
        assert fetchCommittedOffset(host, port, "workers", "orders", 0) == -1;

        ConsumerGroups.Assignment second = joinGroup(host, port, "workers", "orders");
        assert second.generation() > first.generation();
        assert second.partitions().equals(List.of(1));
        try {
            commitOffset(host, port, "workers", "orders", first.memberId(),
                    first.generation(), 0, 1);
            throw new AssertionError("stale generation was accepted");
        } catch (IOException expected) {
            assert expected.getMessage().contains("stale generation");
        }

        first = heartbeat(host, port, "workers", "orders", first.memberId());
        assert first.partitions().equals(List.of(0, 2));
        try {
            commitOffset(host, port, "workers", "orders", first.memberId(),
                    first.generation(), 1, 1);
            throw new AssertionError("unassigned partition was accepted");
        } catch (IOException expected) {
            assert expected.getMessage().contains("unassigned partition");
        }
        commitOffset(host, port, "workers", "orders", first.memberId(),
                first.generation(), 0, 1);
        assert fetchCommittedOffset(host, port, "workers", "orders", 0) == 1;

        ConsumerGroups.Assignment auditors = joinGroup(host, port, "auditors", "orders");
        assert auditors.partitions().equals(List.of(0, 1, 2));
        assert fetchCommittedOffset(host, port, "auditors", "orders", 0) == -1;
        assert fetch(host, port, "orders", 0, 0).get(0).offset() == 0;
        leaveGroup(host, port, "auditors", "orders", auditors.memberId());

        leaveGroup(host, port, "workers", "orders", second.memberId());
        first = heartbeat(host, port, "workers", "orders", first.memberId());
        assert first.partitions().equals(List.of(0, 1, 2));
        leaveGroup(host, port, "workers", "orders", first.memberId());
    }

    private static void groupTimeoutSelfTest(Path directory) {
        AtomicLong now = new AtomicLong(1_000);
        ConsumerGroups groups = new ConsumerGroups(directory, now::get);
        ConsumerGroups.Assignment first = groups.join("timeout", "orders", 3);
        now.addAndGet(ConsumerGroups.SESSION_TIMEOUT_MS);
        try {
            groups.heartbeat("timeout", "orders", first.memberId());
            throw new AssertionError("expired member was accepted");
        } catch (IllegalArgumentException expected) {
            assert expected.getMessage().contains("join again");
        }
        ConsumerGroups.Assignment replacement = groups.join("timeout", "orders", 3);
        assert replacement.partitions().equals(List.of(0, 1, 2));
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

    private static void retentionCompactionSelfTest(Path directory) throws IOException {
        Path compactPath = directory.resolve("compact.log");
        try (PartitionLog log = new PartitionLog(compactPath, 50)) {
            assert log.append(utf8("a"), utf8("old")) == 0;
            assert log.append(null, utf8("keep")) == 1;
            assert log.append(utf8("a"), utf8("new")) == 2;
            assert log.append(utf8("b"), utf8("old")) == 3;
            assert log.append(utf8("b"), null) == 4;
            assert log.append(utf8("a"), utf8("latest")) == 5;
            assert log.append(null, utf8("active")) == 6;
            log.compact();
            List<PartitionLog.Record> remaining = log.readFrom(0);
            assert remaining.stream().map(PartitionLog.Record::offset)
                    .toList().equals(List.of(1L, 4L, 5L, 6L));
            assert remaining.get(1).value() == null;
            assert log.readFrom(3).get(0).offset() == 4;
        }
        assert Files.size(directory.resolve("compact/0.log")) > 0;
        try (PartitionLog reopened = new PartitionLog(compactPath, 50)) {
            assert reopened.readFrom(0).get(0).offset() == 1;
            assert reopened.readFrom(4).get(0).value() == null;
            assert reopened.append(null, utf8("after compaction")) == 7;
        }

        Path retentionPath = directory.resolve("retention.log");
        Path retentionDirectory = directory.resolve("retention");
        try (PartitionLog log = new PartitionLog(retentionPath, 1)) {
            for (int offset = 0; offset < 5; offset++) {
                assert log.append(null, utf8("v" + offset)) == offset;
            }
            long now = System.currentTimeMillis();
            Files.setLastModifiedTime(retentionDirectory.resolve("0.log"),
                    FileTime.fromMillis(now - 10_000));
            Files.setLastModifiedTime(retentionDirectory.resolve("1.log"),
                    FileTime.fromMillis(now - 10_000));
            log.applyRetention(now, 1_000, -1);
            assert !Files.exists(retentionDirectory.resolve("0.log"));
            assert !Files.exists(retentionDirectory.resolve("1.index"));
            assert log.readFrom(0).get(0).offset() == 2;
            log.applyRetention(now, -1, 1);
            assert log.readFrom(0).get(0).offset() == 4;
            assert Files.exists(retentionDirectory.resolve("4.log"));
        }
        try (PartitionLog reopened = new PartitionLog(retentionPath, 1)) {
            assert reopened.readFrom(0).get(0).offset() == 4;
            assert reopened.append(null, utf8("next")) == 5;
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
        System.err.println("  MiniKafka broker <data-dir> [port] [segment-bytes] [retention-ms] [retention-bytes]");
        System.err.println("  MiniKafka create-topic <host> <port> <topic> <partitions>");
        System.err.println("  MiniKafka describe-topic <host> <port> <topic>");
        System.err.println("  MiniKafka produce <host> <port> <topic> <key|-> <value> [partition]");
        System.err.println("  MiniKafka delete <host> <port> <topic> <key> [partition]");
        System.err.println("  MiniKafka consume <host> <port> <topic> <partition> <offset>");
        System.err.println("  MiniKafka consume-group <host> <port> <group> <topic> [polls]");
        System.err.println("  MiniKafka group-offset <host> <port> <group> <topic> <partition>");
        System.err.println("  MiniKafka compact <host> <port> <topic> <partition>");
        System.err.println("  MiniKafka self-test");
        System.exit(2);
    }
}
