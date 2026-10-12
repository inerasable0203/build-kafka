import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

final class ReplicaClient {
    record TopicInfo(String name, int partitions) {}
    record Batch(long leaderEnd, List<PartitionLog.Record> records) {}

    private static final AtomicInteger CORRELATIONS = new AtomicInteger();

    static List<TopicInfo> topics(Cluster.Node leader) throws IOException {
        int correlationId = CORRELATIONS.incrementAndGet();
        ByteArrayOutputStream request = MiniKafka.request(correlationId, Protocol.REPLICA_TOPICS);
        try (DataInputStream response = MiniKafka.exchange(
                leader.host(), leader.port(), request.toByteArray(), correlationId)) {
            int count = response.readInt();
            if (count < 0 || count > 10_000 || count > response.available() / 8) {
                throw new Protocol.ProtocolException("invalid replica topic count");
            }
            List<TopicInfo> topics = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                String name = Protocol.readString(response);
                int partitions = response.readInt();
                if (partitions < 1 || partitions > 1_000) {
                    throw new Protocol.ProtocolException("invalid replica partition count");
                }
                topics.add(new TopicInfo(name, partitions));
            }
            Protocol.requireEnd(response);
            return topics;
        }
    }

    static Batch fetch(Cluster.Node leader, int followerId, String topic, int partition,
            long nextOffset) throws IOException {
        int correlationId = CORRELATIONS.incrementAndGet();
        ByteArrayOutputStream request = MiniKafka.request(correlationId, Protocol.REPLICA_FETCH);
        try (DataOutputStream output = new DataOutputStream(request)) {
            output.writeInt(followerId);
            Protocol.writeString(output, topic);
            output.writeInt(partition);
            output.writeLong(nextOffset);
        }
        try (DataInputStream response = MiniKafka.exchange(
                leader.host(), leader.port(), request.toByteArray(), correlationId)) {
            long leaderEnd = response.readLong();
            if (leaderEnd < nextOffset) {
                throw new Protocol.ProtocolException("follower log is ahead of leader");
            }
            List<PartitionLog.Record> records = MiniKafka.readRecords(response);
            Protocol.requireEnd(response);
            return new Batch(leaderEnd, records);
        }
    }

    private ReplicaClient() {}
}
