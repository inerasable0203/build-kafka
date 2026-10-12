import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

final class Cluster {
    record Node(int id, String host, int port) {}

    private final List<Node> nodes;

    Cluster(List<Node> nodes) {
        if (nodes.size() != 3) throw new IllegalArgumentException("this demo requires 3 brokers");
        for (int id = 0; id < nodes.size(); id++) {
            Node node = nodes.get(id);
            if (node.id() != id || node.host().isBlank()
                    || node.port() < 1 || node.port() > 65_535) {
                throw new IllegalArgumentException("invalid broker entry: " + node);
            }
        }
        this.nodes = List.copyOf(nodes);
    }

    static Cluster read(Path path) throws IOException {
        Properties properties = new Properties();
        try (InputStream input = Files.newInputStream(path)) {
            properties.load(input);
        }
        if (properties.size() != 3) throw new IllegalArgumentException("expected broker.0, broker.1, broker.2");
        List<Node> nodes = new ArrayList<>();
        for (int id = 0; id < 3; id++) {
            String address = properties.getProperty("broker." + id);
            if (address == null) throw new IllegalArgumentException("missing broker." + id);
            int separator = address.lastIndexOf(':');
            if (separator < 1 || separator == address.length() - 1) {
                throw new IllegalArgumentException("invalid broker address: " + address);
            }
            nodes.add(new Node(id, address.substring(0, separator),
                    Integer.parseInt(address.substring(separator + 1))));
        }
        return new Cluster(nodes);
    }

    Node node(int id) {
        if (id < 0 || id >= nodes.size()) throw new IllegalArgumentException("invalid broker id: " + id);
        return nodes.get(id);
    }

    List<Node> nodes() {
        return nodes;
    }

    int leaderId(String topic, int partition) {
        return 0; // ponytail: fixed leader until the controller/failover milestone.
    }
}
