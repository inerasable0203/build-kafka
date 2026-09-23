import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

final class Broker implements AutoCloseable {
    private static final int MAX_PARTITIONS = 1_000;

    private final Path dataDirectory;
    private final long segmentBytes;
    private final ServerSocket server;
    private final ExecutorService workers = Executors.newCachedThreadPool();
    private final Set<Socket> clients = ConcurrentHashMap.newKeySet();
    private final Map<String, Topic> topics = new HashMap<>();
    private volatile boolean running = true;

    Broker(Path dataDirectory, int port) throws IOException {
        this(dataDirectory, port, PartitionLog.DEFAULT_SEGMENT_BYTES);
    }

    Broker(Path dataDirectory, int port, long segmentBytes) throws IOException {
        if (segmentBytes < 1) throw new IllegalArgumentException("segment size must be positive");
        this.dataDirectory = dataDirectory;
        this.segmentBytes = segmentBytes;
        server = new ServerSocket(port);
    }

    int port() {
        return server.getLocalPort();
    }

    void serve() throws IOException {
        while (running) {
            try {
                Socket client = server.accept();
                clients.add(client);
                workers.execute(() -> handle(client));
            } catch (SocketException closed) {
                if (running) throw closed;
            }
        }
    }

    private void handle(Socket client) {
        try (client;
             DataInputStream input = new DataInputStream(client.getInputStream());
             DataOutputStream output = new DataOutputStream(client.getOutputStream())) {
            while (running) {
                byte[] request;
                try {
                    request = Protocol.readFrame(input);
                } catch (EOFException disconnected) {
                    return;
                }
                Protocol.writeFrame(output, respond(request));
            }
        } catch (Protocol.ProtocolException rejected) {
            // Invalid or oversized frames are rejected by closing the connection.
        } catch (IOException error) {
            if (running) System.err.println("client error: " + error.getMessage());
        } finally {
            clients.remove(client);
        }
    }

    private byte[] respond(byte[] request) throws IOException {
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(request))) {
            int correlationId = input.readInt();
            byte apiKey = input.readByte();
            try {
                return switch (apiKey) {
                    case Protocol.PRODUCE -> produce(correlationId, input);
                    case Protocol.FETCH -> fetch(correlationId, input);
                    case Protocol.CREATE_TOPIC -> createTopic(correlationId, input);
                    case Protocol.METADATA -> metadata(correlationId, input);
                    default -> error(correlationId, Protocol.INVALID_REQUEST, "unknown api key: " + apiKey);
                };
            } catch (IllegalArgumentException | EOFException | Protocol.ProtocolException invalid) {
                return error(correlationId, Protocol.INVALID_REQUEST, invalid.getMessage());
            } catch (IOException failure) {
                return error(correlationId, Protocol.SERVER_ERROR, failure.getMessage());
            }
        }
    }

    private byte[] produce(int correlationId, DataInputStream input) throws IOException {
        String topicName = Protocol.readString(input);
        int partition = input.readInt();
        byte[] key = Protocol.readBytes(input, true);
        byte[] value = Protocol.readBytes(input, false);
        Protocol.requireEnd(input);
        long responseRecordBytes = 24L + (key == null ? 0 : key.length) + value.length;
        if (responseRecordBytes > Protocol.MAX_FRAME_SIZE - 9) {
            throw new IllegalArgumentException("record exceeds fetch frame limit");
        }
        long offset = topic(topicName).log(partition).append(key, value);

        ByteArrayOutputStream bytes = success(correlationId);
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            output.writeInt(partition);
            output.writeLong(offset);
        }
        return bytes.toByteArray();
    }

    private byte[] fetch(int correlationId, DataInputStream input) throws IOException {
        String topicName = Protocol.readString(input);
        int partition = input.readInt();
        long offset = input.readLong();
        Protocol.requireEnd(input);

        List<PartitionLog.Record> records = topic(topicName).log(partition)
                .readFrom(offset, Protocol.MAX_FRAME_SIZE - 9);
        ByteArrayOutputStream bytes = success(correlationId);
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            output.writeInt(records.size());
            for (PartitionLog.Record record : records) {
                output.writeLong(record.offset());
                output.writeLong(record.timestamp());
                Protocol.writeNullableBytes(output, record.key());
                Protocol.writeBytes(output, record.value());
            }
        }
        return bytes.toByteArray();
    }

    private byte[] createTopic(int correlationId, DataInputStream input) throws IOException {
        String topic = Protocol.readString(input);
        int partitions = input.readInt();
        Protocol.requireEnd(input);
        createTopic(topic, partitions);
        return success(correlationId).toByteArray();
    }

    private byte[] metadata(int correlationId, DataInputStream input) throws IOException {
        String topicName = Protocol.readString(input);
        Protocol.requireEnd(input);
        int partitions = topic(topicName).partitionCount();

        ByteArrayOutputStream bytes = success(correlationId);
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            output.writeInt(partitions);
        }
        return bytes.toByteArray();
    }

    private static ByteArrayOutputStream success(int correlationId) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream output = new DataOutputStream(bytes);
        output.writeInt(correlationId);
        output.writeByte(Protocol.OK);
        return bytes;
    }

    private static byte[] error(int correlationId, byte status, String message) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            output.writeInt(correlationId);
            output.writeByte(status);
            Protocol.writeString(output, message == null ? "unknown error" : message);
        }
        return bytes.toByteArray();
    }

    private synchronized void createTopic(String name, int partitionCount) throws IOException {
        validateTopicName(name);
        if (partitionCount < 1 || partitionCount > MAX_PARTITIONS) {
            throw new IllegalArgumentException("partition count must be between 1 and " + MAX_PARTITIONS);
        }

        Path directory = dataDirectory.resolve(name);
        Path metadata = directory.resolve("topic.meta");
        Files.createDirectories(directory);
        try {
            Files.writeString(metadata, Integer.toString(partitionCount),
                    StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
        } catch (FileAlreadyExistsException duplicate) {
            throw new IllegalArgumentException("topic already exists: " + name);
        }
        topics.put(name, new Topic(directory, partitionCount, segmentBytes));
    }

    private synchronized Topic topic(String name) throws IOException {
        validateTopicName(name);
        Topic existing = topics.get(name);
        if (existing != null) return existing;

        Path directory = dataDirectory.resolve(name);
        String stored;
        try {
            stored = Files.readString(directory.resolve("topic.meta")).trim();
        } catch (NoSuchFileException missing) {
            throw new IllegalArgumentException("unknown topic: " + name);
        }

        try {
            int partitionCount = Integer.parseInt(stored);
            if (partitionCount < 1 || partitionCount > MAX_PARTITIONS) {
                throw new IOException("invalid metadata for topic: " + name);
            }
            Topic loaded = new Topic(directory, partitionCount, segmentBytes);
            topics.put(name, loaded);
            return loaded;
        } catch (NumberFormatException invalid) {
            throw new IOException("invalid metadata for topic: " + name, invalid);
        }
    }

    private static void validateTopicName(String topic) {
        if (!topic.matches("[A-Za-z0-9._-]+") || topic.equals(".") || topic.equals("..")) {
            throw new IllegalArgumentException("invalid topic name");
        }
    }

    @Override
    public synchronized void close() throws IOException {
        if (!running) return;
        running = false;
        server.close();
        for (Socket client : clients) client.close();
        workers.shutdownNow();

        IOException failure = null;
        for (Topic topic : topics.values()) {
            try {
                topic.close();
            } catch (IOException error) {
                failure = error;
            }
        }
        if (failure != null) throw failure;
    }

    private static final class Topic implements AutoCloseable {
        private final Path directory;
        private final int partitionCount;
        private final long segmentBytes;
        private final Map<Integer, PartitionLog> logs = new HashMap<>();

        Topic(Path directory, int partitionCount, long segmentBytes) {
            this.directory = directory;
            this.partitionCount = partitionCount;
            this.segmentBytes = segmentBytes;
        }

        int partitionCount() {
            return partitionCount;
        }

        synchronized PartitionLog log(int partition) throws IOException {
            if (partition < 0 || partition >= partitionCount) {
                throw new IllegalArgumentException("partition must be between 0 and "
                        + (partitionCount - 1));
            }
            PartitionLog existing = logs.get(partition);
            if (existing != null) return existing;

            PartitionLog created = new PartitionLog(directory.resolve(partition + ".log"), segmentBytes);
            logs.put(partition, created);
            return created;
        }

        @Override
        public synchronized void close() throws IOException {
            IOException failure = null;
            for (PartitionLog log : logs.values()) {
                try {
                    log.close();
                } catch (IOException error) {
                    failure = error;
                }
            }
            if (failure != null) throw failure;
        }
    }
}
