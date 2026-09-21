import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

final class Broker implements AutoCloseable {
    private final Path dataDirectory;
    private final ServerSocket server;
    private final ExecutorService workers = Executors.newCachedThreadPool();
    private final Set<Socket> clients = ConcurrentHashMap.newKeySet();
    private final Map<String, PartitionLog> logs = new HashMap<>();
    private volatile boolean running = true;

    Broker(Path dataDirectory, int port) throws IOException {
        this.dataDirectory = dataDirectory;
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
        String topic = Protocol.readString(input);
        byte[] key = Protocol.readBytes(input, true);
        byte[] value = Protocol.readBytes(input, false);
        Protocol.requireEnd(input);
        long offset = log(topic).append(key, value);

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            output.writeInt(correlationId);
            output.writeByte(Protocol.OK);
            output.writeLong(offset);
        }
        return bytes.toByteArray();
    }

    private byte[] fetch(int correlationId, DataInputStream input) throws IOException {
        String topic = Protocol.readString(input);
        long offset = input.readLong();
        Protocol.requireEnd(input);

        // ponytail: full-log fetch is enough for now; add byte limits with segmented logs.
        List<PartitionLog.Record> records = log(topic).readFrom(offset);
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            output.writeInt(correlationId);
            output.writeByte(Protocol.OK);
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

    private static byte[] error(int correlationId, byte status, String message) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            output.writeInt(correlationId);
            output.writeByte(status);
            Protocol.writeString(output, message == null ? "unknown error" : message);
        }
        return bytes.toByteArray();
    }

    private synchronized PartitionLog log(String topic) throws IOException {
        if (!topic.matches("[A-Za-z0-9._-]+")) throw new IllegalArgumentException("invalid topic name");
        PartitionLog existing = logs.get(topic);
        if (existing != null) return existing;

        PartitionLog created = new PartitionLog(dataDirectory.resolve(topic).resolve("0.log"));
        logs.put(topic, created);
        return created;
    }

    @Override
    public synchronized void close() throws IOException {
        if (!running) return;
        running = false;
        server.close();
        for (Socket client : clients) client.close();
        workers.shutdownNow();

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
