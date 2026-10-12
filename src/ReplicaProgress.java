import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

final class ReplicaProgress {
    private static final long STALE_MS = 5_000;
    private static final long ACK_TIMEOUT_MS = 5_000;

    private record Partition(String topic, int id) {}
    private record Report(long endOffset, long atMillis) {}

    private final Map<Partition, Map<Integer, Report>> reports = new HashMap<>();

    synchronized void report(String topic, int partition, int followerId, long endOffset) {
        reports.computeIfAbsent(new Partition(topic, partition), ignored -> new HashMap<>())
                .put(followerId, new Report(endOffset, System.currentTimeMillis()));
        notifyAll();
    }

    synchronized long endOffset(String topic, int partition, int followerId) {
        Report report = reports.getOrDefault(new Partition(topic, partition), Map.of())
                .get(followerId);
        return report == null ? -1 : report.endOffset();
    }

    synchronized boolean allFollowersAt(String topic, int partition, long targetEnd) {
        return inSync(topic, partition, 1, targetEnd)
                && inSync(topic, partition, 2, targetEnd);
    }

    synchronized boolean inSync(String topic, int partition, int followerId, long leaderEnd) {
        Report report = reports.getOrDefault(new Partition(topic, partition), Map.of())
                .get(followerId);
        return report != null && report.endOffset() >= leaderEnd
                && System.currentTimeMillis() - report.atMillis() < STALE_MS;
    }

    synchronized void awaitAll(String topic, int partition, long targetEnd) throws IOException {
        long deadline = System.currentTimeMillis() + ACK_TIMEOUT_MS;
        while (!allFollowersAt(topic, partition, targetEnd)) {
            long remaining = deadline - System.currentTimeMillis();
            if (remaining <= 0) throw new IOException("replication timeout after local append");
            try {
                wait(remaining);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IOException("replication interrupted after local append", interrupted);
            }
        }
    }
}
