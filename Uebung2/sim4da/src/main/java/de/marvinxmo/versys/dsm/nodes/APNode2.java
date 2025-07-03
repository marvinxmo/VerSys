package de.marvinxmo.versys.dsm.nodes;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import de.marvinxmo.versys.Message;
import de.marvinxmo.versys.dsm.core.CAPType;
import de.marvinxmo.versys.dsm.core.DSMNode;
import de.marvinxmo.versys.dsm.core.DSMSyncMessage;
import de.marvinxmo.versys.dsm.monitoring.ConsistencyMetrics;

/**
 * AP Node Implementation (Availability + Partition Tolerance)
 * 
 * Features:
 * - Local copies of data on each node
 * - Asynchronous write propagation via gossip protocol
 * - Local reads (always available)
 * - Conflict resolution using "Last Write Wins" with logical timestamps
 * - High availability, eventual consistency
 * - Random pauses between operations
 */
public class APNode2 extends DSMNode {

    private final Map<String, VersionedValue> localStorage;
    private final ConsistencyMetrics metrics;
    private volatile long logicalTimestamp;

    // Configuration parameters
    private static final int GOSSIP_FANOUT = 2;
    private static final double GOSSIP_PROBABILITY = 0.7;
    private static final int MIN_PAUSE_MS = 100;
    private static final int MAX_PAUSE_MS = 1000;

    public APNode2(String name) {
        super(name);
        this.localStorage = new ConcurrentHashMap<>();
        this.metrics = new ConsistencyMetrics();
        this.logicalTimestamp = 0;
    }

    /**
     * Represents a value with version information for conflict resolution in AP
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

    @Override
    protected void handleApplicationMessage(Message message) {
        if (isDSMSyncMessage(message)) {
            DSMSyncMessage dsmMessage = parseDSMSyncMessage(message);
            handleSyncMessage(dsmMessage);
        }
    }

    @Override
    protected void engage() {
        // Continuously process incoming messages
        while (true) {
            Message message = receive();
            if (message == null) {
                sleep(10); // Small delay to prevent busy waiting
                continue;
            }

            if (isDSMSyncMessage(message)) {
                // Handle DSM synchronization messages
                if (dsm != null) {
                    try {
                        DSMSyncMessage dsmMessage = parseDSMSyncMessage(message);
                        dsm.handleSyncMessage(dsmMessage);
                    } catch (Exception e) {
                        System.err.printf("[%s] Error handling DSM message: %s%n",
                                NodeName(), e.getMessage());
                    }
                }
            } else {
                // Handle other application-specific messages
                handleApplicationMessage(message);
            }
        }
    }

    /**
     * Write a value to the distributed shared memory
     */
    public void writeValue(String key, String value) {
        long startTime = System.currentTimeMillis();

        try {
            // Random pause before operation
            randomPause();

            // Increment logical timestamp
            logicalTimestamp++;

            // Store locally with timestamp and origin
            VersionedValue versionedValue = new VersionedValue(value, logicalTimestamp, getName());
            localStorage.put(key, versionedValue);

            System.out.printf("[%s] 📝 AP WRITE: %s = %s (timestamp: %d)%n",
                    getName(), key, value, logicalTimestamp);

            // Asynchronously propagate to other nodes via gossip
            propagateWrite(key, value, logicalTimestamp);

            metrics.recordWrite(true, System.currentTimeMillis() - startTime);
        } catch (Exception e) {
            metrics.recordWrite(false, System.currentTimeMillis() - startTime);
            System.err.printf("[%s] ❌ AP Write failed: %s%n", getName(), e.getMessage());
        }
    }

    /**
     * Read a value from the distributed shared memory
     */
    public String readValue(String key) {
        long startTime = System.currentTimeMillis();

        try {
            // Random pause before operation
            randomPause();

            // Always read from local storage (high availability)
            VersionedValue versionedValue = localStorage.get(key);
            String value = (versionedValue != null) ? versionedValue.value : null;

            System.out.printf("[%s] 📖 AP READ: %s = %s%n", getName(), key, value);

            // Optionally trigger gossip to improve consistency
            if (random.nextDouble() < GOSSIP_PROBABILITY) {
                triggerGossipSync();
            }

            metrics.recordRead(true, true, System.currentTimeMillis() - startTime);
            return value;
        } catch (Exception e) {
            metrics.recordRead(false, false, System.currentTimeMillis() - startTime);
            System.err.printf("[%s] ❌ AP Read failed: %s%n", getName(), e.getMessage());
            return null;
        }
    }

    /**
     * Handle incoming synchronization messages
     */
    private void handleSyncMessage(DSMSyncMessage message) {
        // Update logical timestamp
        logicalTimestamp = Math.max(logicalTimestamp, message.getTimestamp()) + 1;

        switch (message.getType()) {
            case WRITE_PROPAGATION:
                handleWritePropagation(message);
                break;
            case GOSSIP_SYNC:
                handleGossipSync(message);
                break;
            default:
                // Ignore unknown message types
                break;
        }
    }

    /**
     * Get the CAP type of this node
     */
    public CAPType getCAPType() {
        return CAPType.AP;
    }

    /**
     * Get consistency metrics
     */
    public ConsistencyMetrics getMetrics() {
        return metrics;
    }

    /**
     * Get current local state
     */
    public Map<String, String> getLocalState() {
        Map<String, String> state = new HashMap<>();
        for (Map.Entry<String, VersionedValue> entry : localStorage.entrySet()) {
            state.put(entry.getKey(), entry.getValue().value);
        }
        return state;
    }

    /**
     * Random pause between operations
     */
    private void randomPause() {
        try {
            int pauseMs = MIN_PAUSE_MS + random.nextInt(MAX_PAUSE_MS - MIN_PAUSE_MS);
            Thread.sleep(pauseMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
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
                    getName());

            try {
                sendMessageBlindly(syncMessage, targetNode);
                System.out.printf("[%s] 📤 Propagating write to %s: %s = %s%n",
                        getName(), targetNode, key, value);
            } catch (Exception e) {
                // Network partition or node failure - continue anyway (AP guarantees)
                System.out.printf("[%s] ❌ Failed to propagate to %s: %s%n",
                        getName(), targetNode, e.getMessage());
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
            if (shouldUpdate) {
                System.out.printf("[%s] 🔍 CONSISTENCY NOTE: Timestamp conflict resolved%n", getName());
                metrics.recordInconsistency("Timestamp conflict resolved using node ID");
            }
        }

        if (shouldUpdate) {
            VersionedValue newValue = new VersionedValue(incomingValue, incomingTimestamp, originNodeId);
            localStorage.put(key, newValue);

            System.out.printf("[%s] 🔄 Updated from propagation: %s = %s (timestamp: %d, origin: %s)%n",
                    getName(), key, incomingValue, incomingTimestamp, originNodeId);

            // Continue gossip propagation with reduced probability to avoid flooding
            if (random.nextDouble() < GOSSIP_PROBABILITY * 0.5) {
                propagateWrite(key, incomingValue, incomingTimestamp);
            }
        } else {
            System.out.printf("[%s] 🚫 Ignored older write: %s = %s (timestamp: %d)%n",
                    getName(), key, incomingValue, incomingTimestamp);
        }
    }

    /**
     * Trigger periodic gossip synchronization
     */
    private void triggerGossipSync() {
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
                        sendMessageBlindly(syncMessage, targetNode);
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

            System.out.printf("[%s] 🔄 Synced via gossip: %s = %s (timestamp: %d, origin: %s)%n",
                    getName(), key, incomingValue, incomingTimestamp, originNodeId);
        }
    }

    /**
     * Select random subset of nodes for gossip communication
     */
    private List<String> selectGossipTargets() {
        List<String> availableNodes = new ArrayList<>();
        for (String nodeId : knownNodes) {
            if (!nodeId.equals(getName())) {
                availableNodes.add(nodeId);
            }
        }

        Collections.shuffle(availableNodes, random);
        int fanout = Math.min(GOSSIP_FANOUT, availableNodes.size());
        return availableNodes.subList(0, fanout);
    }

    /**
     * Check if a message is a DSM synchronization message
     */
    private boolean isDSMSyncMessage(Message message) {
        return message.query("type") != null &&
                message.query("key") != null &&
                message.query("timestamp") != null &&
                message.query("originNodeId") != null;
    }

    /**
     * Parse a regular Message into a DSMSyncMessage
     */
    private DSMSyncMessage parseDSMSyncMessage(Message message) {
        DSMSyncMessage.Type type = DSMSyncMessage.Type.valueOf(message.query("type"));
        String key = message.query("key");
        String value = message.query("value");
        long timestamp = Long.parseLong(message.query("timestamp"));
        String originNodeId = message.query("originNodeId");
        String messageId = message.query("messageId");
        boolean requiresResponse = Boolean.parseBoolean(message.query("requiresResponse"));

        return new DSMSyncMessage(type, key, value, timestamp, originNodeId, messageId, requiresResponse);
    }
}
