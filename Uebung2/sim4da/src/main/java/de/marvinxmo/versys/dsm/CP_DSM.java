package de.marvinxmo.versys.dsm;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * CP DSM Implementation (Consistency + Partition Tolerance)
 * 
 * Features:
 * - Quorum-based reads and writes for strong consistency
 * - Operations may block when quorum is not available
 * - Partition tolerant but may sacrifice availability
 * - Uses majority quorum for consistency guarantees
 */
public class CP_DSM implements DSM {

    private final DSMNode node;
    private final Map<String, VersionedValue> localStorage;
    private final Set<String> knownNodes;
    private long logicalTimestamp;
    private final int quorumSize;
    private final Map<String, Set<String>> pendingWrites; // Track pending write confirmations
    private final Map<String, AtomicInteger> writeConfirmations; // Count confirmations per write

    /**
     * Represents a value with version information for conflict resolution
     */
    private static class VersionedValue {
        final String value;
        final long timestamp;
        final String originNodeId;

        VersionedValue(String value, long timestamp, String originNodeId) {
            this.value = value;
            this.timestamp = timestamp;
            this.originNodeId = originNodeId;
        }

        @Override
        public String toString() {
            return String.format("VersionedValue[value=%s, timestamp=%d, origin=%s]",
                    value, timestamp, originNodeId);
        }
    }

    public CP_DSM(DSMNode node, Set<String> knownNodes) {
        this.node = node;
        this.localStorage = new ConcurrentHashMap<>();
        this.knownNodes = new HashSet<>(knownNodes);
        this.logicalTimestamp = 0;
        this.quorumSize = (knownNodes.size() / 2) + 1; // Majority quorum
        this.pendingWrites = new ConcurrentHashMap<>();
        this.writeConfirmations = new ConcurrentHashMap<>();

        System.out.printf("[%s] CP DSM initialized with quorum size: %d (total nodes: %d)%n",
                node.getName(), quorumSize, knownNodes.size());
    }

    @Override
    public void write(String key, String value) {
        logicalTimestamp++;

        // Generate unique write ID for tracking confirmations
        String writeId = node.getName() + "_" + logicalTimestamp + "_" + key;

        // Store locally first
        VersionedValue versionedValue = new VersionedValue(value, logicalTimestamp, node.getName());
        localStorage.put(key, versionedValue);

        // Initialize confirmation tracking
        pendingWrites.put(writeId, new HashSet<>());
        writeConfirmations.put(writeId, new AtomicInteger(1)); // Count self

        System.out.printf("[%s] CP Write initiated: %s = %s (timestamp: %d, writeId: %s)%n",
                node.getName(), key, value, logicalTimestamp, writeId);

        // Send write requests to all other nodes
        boolean quorumAchieved = requestWriteQuorum(key, value, logicalTimestamp, writeId);

        if (quorumAchieved) {
            System.out.printf("[%s] Write COMMITTED: %s = %s (quorum achieved)%n",
                    node.getName(), key, value);
        } else {
            // Remove the local write if quorum failed
            localStorage.remove(key);
            System.err.printf("[%s] Write ABORTED: %s = %s (quorum failed)%n",
                    node.getName(), key, value);
            throw new RuntimeException("Write failed - insufficient quorum");
        }

        // Cleanup tracking
        pendingWrites.remove(writeId);
        writeConfirmations.remove(writeId);
    }

    @Override
    public String read(String key) {
        // For CP system, we need to ensure we read the most recent committed value
        // This simplified implementation reads from local storage
        // In a full implementation, this would also require a read quorum

        VersionedValue versionedValue = localStorage.get(key);
        String value = (versionedValue != null) ? versionedValue.value : null;

        System.out.printf("[%s] CP Read: %s = %s%n", node.getName(), key, value);

        // In a full CP implementation, we might need to verify this with a read quorum
        // For simplicity, we assume local reads are sufficient if the write quorum was
        // achieved

        return value;
    }

    @Override
    public void handleSyncMessage(DSMSyncMessage message) {
        logicalTimestamp = Math.max(logicalTimestamp, message.getTimestamp()) + 1;

        switch (message.getType()) {
            case WRITE_PROPAGATION:
                handleWriteRequest(message);
                break;
            case GOSSIP_SYNC:
                handleWriteConfirmation(message);
                break;
        }
    }

    @Override
    public long getLogicalTimestamp() {
        return logicalTimestamp;
    }

    /**
     * Request write quorum from other nodes
     */
    private boolean requestWriteQuorum(String key, String value, long timestamp, String writeId) {
        // Send write requests to all other nodes
        for (String targetNode : knownNodes) {
            if (!targetNode.equals(node.getName())) {
                DSMSyncMessage writeRequest = new DSMSyncMessage(
                        DSMSyncMessage.Type.WRITE_PROPAGATION,
                        key,
                        value + "|" + writeId, // Include writeId in value for tracking
                        timestamp,
                        node.getName());

                try {
                    node.sendMessageBlindly(writeRequest, targetNode);
                    System.out.printf("[%s] Write request sent to %s for key %s%n",
                            node.getName(), targetNode, key);
                } catch (Exception e) {
                    System.err.printf("[%s] Failed to send write request to %s: %s%n",
                            node.getName(), targetNode, e.getMessage());
                }
            }
        }

        // Wait for confirmations (simplified - in real implementation this would be
        // event-driven)
        int maxWaitTime = 5000; // 5 seconds timeout
        int waitIncrement = 100; // Check every 100ms
        int totalWait = 0;

        while (totalWait < maxWaitTime) {
            AtomicInteger confirmCount = writeConfirmations.get(writeId);
            if (confirmCount != null && confirmCount.get() >= quorumSize) {
                return true; // Quorum achieved
            }

            try {
                Thread.sleep(waitIncrement);
                totalWait += waitIncrement;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }

        System.err.printf("[%s] Quorum timeout for write %s (got %d/%d confirmations)%n",
                node.getName(), writeId,
                writeConfirmations.getOrDefault(writeId, new AtomicInteger(0)).get(),
                quorumSize);
        return false; // Timeout - quorum not achieved
    }

    /**
     * Handle incoming write request from another node
     */
    private void handleWriteRequest(DSMSyncMessage message) {
        String key = message.getKey();
        String valueWithId = message.getValue();
        long timestamp = message.getTimestamp();
        String originNodeId = message.getOriginNodeId();

        // Extract actual value and writeId
        String[] parts = valueWithId.split("\\|");
        String value = parts[0];
        String writeId = parts.length > 1 ? parts[1] : "unknown";

        // Store the value with version info
        VersionedValue versionedValue = new VersionedValue(value, timestamp, originNodeId);
        localStorage.put(key, versionedValue);

        System.out.printf("[%s] Accepted write request: %s = %s (timestamp: %d, from: %s)%n",
                node.getName(), key, value, timestamp, originNodeId);

        // Send confirmation back to the originator
        DSMSyncMessage confirmation = new DSMSyncMessage(
                DSMSyncMessage.Type.GOSSIP_SYNC,
                key,
                writeId, // Send writeId as confirmation
                timestamp,
                node.getName());

        try {
            node.sendMessageBlindly(confirmation, originNodeId);
            System.out.printf("[%s] Confirmation sent to %s for writeId %s%n",
                    node.getName(), originNodeId, writeId);
        } catch (Exception e) {
            System.err.printf("[%s] Failed to send confirmation to %s: %s%n",
                    node.getName(), originNodeId, e.getMessage());
        }
    }

    /**
     * Handle write confirmation from another node
     */
    private void handleWriteConfirmation(DSMSyncMessage message) {
        String writeId = message.getValue(); // writeId is stored in value field
        String confirmingNode = message.getOriginNodeId();

        AtomicInteger confirmCount = writeConfirmations.get(writeId);
        if (confirmCount != null) {
            int newCount = confirmCount.incrementAndGet();
            Set<String> confirmedNodes = pendingWrites.get(writeId);
            if (confirmedNodes != null) {
                confirmedNodes.add(confirmingNode);
            }

            System.out.printf("[%s] Received confirmation from %s for writeId %s (total: %d/%d)%n",
                    node.getName(), confirmingNode, writeId, newCount, quorumSize);
        }
    }

    /**
     * Get current local state for debugging/monitoring
     */
    public Map<String, String> getLocalState() {
        Map<String, String> state = new HashMap<>();
        for (Map.Entry<String, VersionedValue> entry : localStorage.entrySet()) {
            state.put(entry.getKey(), entry.getValue().value);
        }
        return state;
    }

    /**
     * Get detailed local state with version information
     */
    public Map<String, VersionedValue> getDetailedLocalState() {
        return new HashMap<>(localStorage);
    }

    /**
     * Get the required quorum size
     */
    public int getQuorumSize() {
        return quorumSize;
    }
}
