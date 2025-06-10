package de.marvinxmo.versys.dsm;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * AP DSM Implementation (Availability + Partition Tolerance)
 * 
 * Features:
 * - Local copies of data on each node
 * - Asynchronous write propagation via gossip protocol
 * - Local reads (always available)
 * - Conflict resolution using "Last Write Wins" with logical timestamps
 * - High availability, eventual consistency
 */
public class AP_DSM implements DSM {

    private final DSMNode node;
    private final Map<String, VersionedValue> localStorage;
    private final Set<String> knownNodes;
    private long logicalTimestamp;
    private final Random random;

    // Configuration parameters
    private static final int GOSSIP_FANOUT = 2; // Number of nodes to gossip to
    private static final double GOSSIP_PROBABILITY = 0.7; // Probability of gossiping

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

    public AP_DSM(DSMNode node, Set<String> knownNodes) {
        this.node = node;
        this.localStorage = new ConcurrentHashMap<>();
        this.knownNodes = new HashSet<>(knownNodes);
        this.logicalTimestamp = 0;
        this.random = new Random();
    }

    @Override
    public void write(String key, String value) {
        // Increment logical timestamp
        logicalTimestamp++;

        // Store locally with timestamp and origin
        VersionedValue versionedValue = new VersionedValue(value, logicalTimestamp, node.getName());
        localStorage.put(key, versionedValue);

        // Log the write operation
        System.out.printf("[%s] Local write: %s = %s (timestamp: %d)%n",
                node.getName(), key, value, logicalTimestamp);

        // Asynchronously propagate to other nodes via gossip
        propagateWrite(key, value, logicalTimestamp);
    }

    @Override
    public String read(String key) {
        // Always read from local storage (high availability)
        VersionedValue versionedValue = localStorage.get(key);
        String value = (versionedValue != null) ? versionedValue.value : null;

        System.out.printf("[%s] Local read: %s = %s%n", node.getName(), key, value);

        // Optionally trigger gossip to improve consistency
        if (random.nextDouble() < GOSSIP_PROBABILITY) {
            triggerGossipSync();
        }

        return value;
    }

    @Override
    public void handleSyncMessage(DSMSyncMessage message) {
        // Update logical timestamp
        logicalTimestamp = Math.max(logicalTimestamp, message.getTimestamp()) + 1;

        switch (message.getType()) {
            case WRITE_PROPAGATION:
                handleWritePropagation(message);
                break;
            case GOSSIP_SYNC:
                handleGossipSync(message);
                break;
        }
    }

    @Override
    public long getLogicalTimestamp() {
        return logicalTimestamp;
    }

    /**
     * Propagate a write operation to other nodes using gossip protocol
     */
    private void propagateWrite(String key, String value, long timestamp) {
        List<String> targetNodes = selectGossipTargets();
        for (String targetNode : targetNodes) {
            DSMSyncMessage syncMessage = new DSMSyncMessage(
                    DSMSyncMessage.Type.WRITE_PROPAGATION,
                    key,
                    value,
                    timestamp,
                    node.getName()); // Send asynchronously (fire and forget)
            try {
                node.sendMessageBlindly(syncMessage, targetNode);
                System.out.printf("[%s] Propagating write to %s: %s = %s%n",
                        node.getName(), targetNode, key, value);
            } catch (Exception e) {
                // Network partition or node failure - continue anyway (AP guarantees)
                System.out.printf("[%s] Failed to propagate to %s: %s%n",
                        node.getName(), targetNode, e.getMessage());
            }
        }
    }

    /**
     * Handle incoming write propagation messages
     */
    private void handleWritePropagation(DSMSyncMessage message) {
        String key = message.getKey();
        String incomingValue = message.getValue();
        long incomingTimestamp = message.getTimestamp();
        String originNodeId = message.getOriginNodeId();

        // Conflict resolution: Last Write Wins based on logical timestamps
        VersionedValue currentValue = localStorage.get(key);

        boolean shouldUpdate = false;
        if (currentValue == null) {
            shouldUpdate = true;
        } else if (incomingTimestamp > currentValue.timestamp) {
            shouldUpdate = true;
        } else if (incomingTimestamp == currentValue.timestamp) {
            // Tie-breaker: use node ID lexicographic order
            shouldUpdate = originNodeId.compareTo(currentValue.originNodeId) > 0;
        }

        if (shouldUpdate) {
            VersionedValue newValue = new VersionedValue(incomingValue, incomingTimestamp, originNodeId);
            localStorage.put(key, newValue);

            System.out.printf("[%s] Updated from propagation: %s = %s (timestamp: %d, origin: %s)%n",
                    node.getName(), key, incomingValue, incomingTimestamp, originNodeId);

            // Continue gossip propagation with some probability
            if (random.nextDouble() < GOSSIP_PROBABILITY * 0.5) { // Reduced probability to avoid flooding
                propagateWrite(key, incomingValue, incomingTimestamp);
            }
        } else {
            System.out.printf("[%s] Ignored older write: %s = %s (timestamp: %d)%n",
                    node.getName(), key, incomingValue, incomingTimestamp);
        }
    }

    /**
     * Trigger periodic gossip synchronization
     */
    private void triggerGossipSync() {
        // For simplicity, we'll just trigger additional write propagations
        // In a full implementation, this would exchange state summaries
        List<String> targetNodes = selectGossipTargets();
        for (String targetNode : targetNodes) {
            // Send a subset of our local state for synchronization
            for (Map.Entry<String, VersionedValue> entry : localStorage.entrySet()) {
                if (random.nextDouble() < 0.3) { // Only sync some entries to avoid overhead
                    VersionedValue versionedValue = entry.getValue();
                    DSMSyncMessage syncMessage = new DSMSyncMessage(
                            DSMSyncMessage.Type.GOSSIP_SYNC,
                            entry.getKey(),
                            versionedValue.value,
                            versionedValue.timestamp,
                            versionedValue.originNodeId);
                    try {
                        node.sendMessageBlindly(syncMessage, targetNode);
                    } catch (Exception e) {
                        // Ignore network failures (AP guarantees)
                    }
                }
            }
        }
    }

    /**
     * Handle gossip synchronization messages
     */
    private void handleGossipSync(DSMSyncMessage message) {
        // Same logic as write propagation, but without further propagation
        String key = message.getKey();
        String incomingValue = message.getValue();
        long incomingTimestamp = message.getTimestamp();
        String originNodeId = message.getOriginNodeId();

        VersionedValue currentValue = localStorage.get(key);

        boolean shouldUpdate = false;
        if (currentValue == null) {
            shouldUpdate = true;
        } else if (incomingTimestamp > currentValue.timestamp) {
            shouldUpdate = true;
        } else if (incomingTimestamp == currentValue.timestamp) {
            shouldUpdate = originNodeId.compareTo(currentValue.originNodeId) > 0;
        }

        if (shouldUpdate) {
            VersionedValue newValue = new VersionedValue(incomingValue, incomingTimestamp, originNodeId);
            localStorage.put(key, newValue);

            System.out.printf("[%s] Synced via gossip: %s = %s (timestamp: %d, origin: %s)%n",
                    node.getName(), key, incomingValue, incomingTimestamp, originNodeId);
        }
    }

    /**
     * Select random subset of nodes for gossip communication
     */
    private List<String> selectGossipTargets() {
        List<String> availableNodes = new ArrayList<>();
        for (String nodeId : knownNodes) {
            if (!nodeId.equals(node.getName())) {
                availableNodes.add(nodeId);
            }
        }

        Collections.shuffle(availableNodes, random);
        int fanout = Math.min(GOSSIP_FANOUT, availableNodes.size());
        return availableNodes.subList(0, fanout);
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
}