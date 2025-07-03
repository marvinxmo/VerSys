package de.marvinxmo.versys.dsm.old;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * CA DSM Implementation (Consistency + Availability)
 * 
 * Features:
 * - Centralized coordination (assumes no network partitions)
 * - Synchronous operations with immediate consistency
 * - All nodes maintain synchronized state
 * - High consistency and availability, but no partition tolerance
 */
public class CA_DSM implements DSM {

    private final DSMNode node;
    private final Map<String, String> localStorage;
    private final Set<String> knownNodes;
    private long logicalTimestamp;
    private final boolean isCentralCoordinator;

    /**
     * Constructor for CA DSM
     * 
     * @param node                 The DSM node
     * @param knownNodes           Set of all known nodes in the system
     * @param isCentralCoordinator Whether this node acts as the central coordinator
     */
    public CA_DSM(DSMNode node, Set<String> knownNodes, boolean isCentralCoordinator) {
        this.node = node;
        this.localStorage = new ConcurrentHashMap<>();
        this.knownNodes = new HashSet<>(knownNodes);
        this.logicalTimestamp = 0;
        this.isCentralCoordinator = isCentralCoordinator;
    }

    @Override
    public void write(String key, String value) {
        logicalTimestamp++;

        if (isCentralCoordinator) {
            // Central coordinator handles the write
            handleCentralizedWrite(key, value);
        } else {
            // Non-coordinator nodes forward write requests to coordinator
            forwardWriteToCoordinator(key, value);
        }

        System.out.printf("[%s] CA Write: %s = %s (timestamp: %d)%n",
                node.getName(), key, value, logicalTimestamp);
    }

    @Override
    public String read(String key) {
        // In CA system, reads are always consistent and immediately available
        String value = localStorage.get(key);

        System.out.printf("[%s] CA Read: %s = %s%n", node.getName(), key, value);

        return value;
    }

    @Override
    public void handleSyncMessage(DSMSyncMessage message) {
        logicalTimestamp = Math.max(logicalTimestamp, message.getTimestamp()) + 1;

        switch (message.getType()) {
            case WRITE_PROPAGATION:
                handleSyncWrite(message);
                break;
            case GOSSIP_SYNC:
                // CA system doesn't use gossip, treat as regular sync
                handleSyncWrite(message);
                break;
        }
    }

    @Override
    public long getLogicalTimestamp() {
        return logicalTimestamp;
    }

    /**
     * Handle centralized write operation (coordinator only)
     */
    private void handleCentralizedWrite(String key, String value) {
        // Update local storage immediately
        localStorage.put(key, value);

        // Synchronously propagate to all other nodes
        propagateWriteSync(key, value, logicalTimestamp);

        System.out.printf("[%s] Coordinator processed write: %s = %s%n",
                node.getName(), key, value);
    }

    /**
     * Forward write request to central coordinator
     */
    private void forwardWriteToCoordinator(String key, String value) {
        // Find coordinator node (assuming first node in alphabetical order is
        // coordinator)
        String coordinatorId = knownNodes.stream().min(String::compareTo).orElse(node.getName());

        if (!coordinatorId.equals(node.getName())) {
            DSMSyncMessage writeRequest = new DSMSyncMessage(
                    DSMSyncMessage.Type.WRITE_PROPAGATION,
                    key,
                    value,
                    logicalTimestamp,
                    node.getName());

            try {
                node.sendMessage(writeRequest, coordinatorId);
                System.out.printf("[%s] Forwarded write request to coordinator %s%n",
                        node.getName(), coordinatorId);
            } catch (Exception e) {
                System.err.printf("[%s] Failed to forward write to coordinator: %s%n",
                        node.getName(), e.getMessage());
                // In CA system, this is a critical failure
                throw new RuntimeException("CA System failed - coordinator unreachable", e);
            }
        }
    }

    /**
     * Synchronously propagate write to all nodes
     */
    private void propagateWriteSync(String key, String value, long timestamp) {
        for (String targetNode : knownNodes) {
            if (!targetNode.equals(node.getName())) {
                DSMSyncMessage syncMessage = new DSMSyncMessage(
                        DSMSyncMessage.Type.WRITE_PROPAGATION,
                        key,
                        value,
                        timestamp,
                        node.getName());

                try {
                    node.sendMessage(syncMessage, targetNode);
                    System.out.printf("[%s] Sync write sent to %s: %s = %s%n",
                            node.getName(), targetNode, key, value);
                } catch (Exception e) {
                    System.err.printf("[%s] Failed to sync write to %s: %s%n",
                            node.getName(), targetNode, e.getMessage());
                    // In CA system, any network failure is critical
                    throw new RuntimeException("CA System failed - network partition detected", e);
                }
            }
        }
    }

    /**
     * Handle incoming synchronized write
     */
    private void handleSyncWrite(DSMSyncMessage message) {
        String key = message.getKey();
        String value = message.getValue();
        long timestamp = message.getTimestamp();

        // In CA system, all writes are immediately applied (strong consistency)
        localStorage.put(key, value);

        System.out.printf("[%s] Applied sync write: %s = %s (timestamp: %d)%n",
                node.getName(), key, value, timestamp);

        // Send acknowledgment back to coordinator if this was a forwarded write
        if (isCentralCoordinator && !message.getOriginNodeId().equals(node.getName())) {
            // Coordinator received a write request from another node
            // The write has been applied, propagate to other nodes
            propagateWriteSync(key, value, timestamp);
        }
    }

    /**
     * Get current local state for debugging/monitoring
     */
    public Map<String, String> getLocalState() {
        return new HashMap<>(localStorage);
    }

    /**
     * Check if this node is the central coordinator
     */
    public boolean isCentralCoordinator() {
        return isCentralCoordinator;
    }
}
