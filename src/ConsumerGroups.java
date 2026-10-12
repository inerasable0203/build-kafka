import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.LongSupplier;

final class ConsumerGroups {
    static final long SESSION_TIMEOUT_MS = 10_000;
    private static final int MAX_MEMBERS = 1_000;

    record Assignment(String memberId, int generation, List<Integer> partitions) {}
    private record GroupKey(String group, String topic) {}

    private static final class Group {
        final int partitionCount;
        final Map<String, Long> members = new LinkedHashMap<>();
        int generation;

        Group(int partitionCount) {
            this.partitionCount = partitionCount;
        }
    }

    private final Path dataDirectory;
    private final LongSupplier clock;
    private final Map<GroupKey, Group> groups = new HashMap<>();

    ConsumerGroups(Path dataDirectory) {
        this(dataDirectory, System::currentTimeMillis);
    }

    ConsumerGroups(Path dataDirectory, LongSupplier clock) {
        this.dataDirectory = dataDirectory;
        this.clock = clock;
    }

    synchronized Assignment join(String groupId, String topic, int partitionCount) {
        validateName(groupId, "group");
        GroupKey key = new GroupKey(groupId, topic);
        Group group = groups.computeIfAbsent(key, ignored -> new Group(partitionCount));
        expire(group);
        if (group.members.size() >= MAX_MEMBERS) {
            throw new IllegalArgumentException("group is full");
        }
        String memberId = UUID.randomUUID().toString();
        group.members.put(memberId, clock.getAsLong());
        group.generation++;
        return assignment(group, memberId);
    }

    synchronized Assignment heartbeat(String groupId, String topic, String memberId) {
        Group group = activeGroup(groupId, topic, memberId);
        group.members.put(memberId, clock.getAsLong());
        return assignment(group, memberId);
    }

    synchronized void leave(String groupId, String topic, String memberId) {
        Group group = activeGroup(groupId, topic, memberId);
        group.members.remove(memberId);
        group.generation++;
    }

    synchronized void commit(String groupId, String topic, String memberId, int generation,
            int partition, long nextOffset) throws IOException {
        if (nextOffset < 0) throw new IllegalArgumentException("next offset must be >= 0");
        Group group = activeGroup(groupId, topic, memberId);
        if (generation != group.generation || !assignment(group, memberId).partitions().contains(partition)) {
            throw new IllegalArgumentException("stale generation or unassigned partition");
        }
        Path target = offsetPath(groupId, topic, partition);
        Files.createDirectories(target.getParent());
        Path temporary = Files.createTempFile(target.getParent(), "offset-", ".tmp");
        try {
            try (FileChannel file = FileChannel.open(temporary, StandardOpenOption.WRITE)) {
                ByteBuffer bytes = ByteBuffer.allocate(Long.BYTES).putLong(nextOffset);
                bytes.flip();
                while (bytes.hasRemaining()) file.write(bytes);
                file.force(true);
            }
            Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING);
        } finally {
            Files.deleteIfExists(temporary);
        }
    }

    synchronized long fetchOffset(String groupId, String topic, int partition) throws IOException {
        Path path = offsetPath(groupId, topic, partition);
        byte[] bytes;
        try {
            bytes = Files.readAllBytes(path);
        } catch (NoSuchFileException missing) {
            return -1;
        }
        if (bytes.length != Long.BYTES) throw new IOException("invalid committed offset file: " + path);
        long offset = ByteBuffer.wrap(bytes).getLong();
        if (offset < 0) throw new IOException("invalid committed offset: " + offset);
        return offset;
    }

    private Group activeGroup(String groupId, String topic, String memberId) {
        validateName(groupId, "group");
        Group group = groups.get(new GroupKey(groupId, topic));
        if (group == null) throw new IllegalArgumentException("unknown group member; join again");
        expire(group);
        if (!group.members.containsKey(memberId)) {
            throw new IllegalArgumentException("unknown group member; join again");
        }
        return group;
    }

    private void expire(Group group) {
        long now = clock.getAsLong();
        int previousSize = group.members.size();
        group.members.values().removeIf(lastHeartbeat -> now - lastHeartbeat >= SESSION_TIMEOUT_MS);
        if (group.members.size() != previousSize) group.generation++;
    }

    private static Assignment assignment(Group group, String memberId) {
        List<String> members = new ArrayList<>(group.members.keySet());
        int memberIndex = members.indexOf(memberId);
        List<Integer> partitions = new ArrayList<>();
        for (int partition = memberIndex; partition < group.partitionCount;
                partition += members.size()) {
            partitions.add(partition);
        }
        return new Assignment(memberId, group.generation, List.copyOf(partitions));
    }

    private Path offsetPath(String groupId, String topic, int partition) {
        validateName(groupId, "group");
        if (partition < 0) throw new IllegalArgumentException("partition must be >= 0");
        return dataDirectory.resolve(topic).resolve(".consumer-offsets").resolve(groupId)
                .resolve(partition + ".offset");
    }

    private static void validateName(String name, String label) {
        if (name == null || name.length() > 128 || !name.matches("[A-Za-z0-9._-]+")
                || name.equals(".") || name.equals("..")) {
            throw new IllegalArgumentException("invalid " + label + " name");
        }
    }
}
