package de.marvinxmo.versys.dsm.nodes;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import de.marvinxmo.versys.Message;
import de.marvinxmo.versys.UnknownNodeException;
import de.marvinxmo.versys.dsm.core.CAPType;
import de.marvinxmo.versys.dsm.core.DSMNode;
import de.marvinxmo.versys.dsm.core.DSMSyncMessage;
import de.marvinxmo.versys.dsm.monitoring.ConsistencyMetrics;

/**
 * CA Node Implementation (Consistency + Availability)
 * 
 * Features:
 * - Centralized coordination (assumes no network partitions)
 * - Synchronous operations with immediate consistency
 * - All nodes maintain synchronized state
 * - High consistency and availability, but no partition tolerance
 * - Random pauses between operations
 */
public class CANode2 extends DSMNode {

    private final Map<String, String> localStorage;
    private final Set<String> knownNodes;
    private final boolean isCentralCoordinator;
    private final String coordinatorNodeId;
    private final Random random;
    private final ConsistencyMetrics metrics;
    private volatile long logicalTimestamp;

    // Configuration parameters
    private static final int MIN_PAUSE_MS = 100;
    private static final int MAX_PAUSE_MS = 800;

    public CANode2(String name, Set<String> knownNodes, boolean isCentralCoordinator) {
        super(name);
        this.localStorage = new ConcurrentHashMap<>();
        this.knownNodes = knownNodes;
        this.isCentralCoordinator = isCentralCoordinator;
        this.coordinatorNodeId = determineCoordinator(knownNodes);
        this.random = new Random();
        this.metrics = new ConsistencyMetrics();
        this.logicalTimestamp = 0;
    }

    @Override
    protected void handleApplicationMessage(Message message) {
        if (isDSMSyncMessage(message)) {
            DSMSyncMessage dsmMessage = parseDSMSyncMessage(message);
            handleSyncMessage(dsmMessage);
        }
    }

    /**
     * Write a value to the distributed shared memory
     */
    public void writeValue(String key, String value) throws Exception {
        long startTime = System.currentTimeMillis();

        if (isPartitioned()) {
            metrics.recordWrite(false, System.currentTimeMillis() - startTime);
            throw new Exception("CA system unavailable during partition");
        }

        try {
            // Random pause before operation
            randomPause();

            logicalTimestamp++;

            if (isCentralCoordinator) {
                // Central coordinator handles the write
                handleCentralizedWrite(key, value);
            } else {
                // Non-coordinator nodes forward write requests to coordinator
                forwardWriteToCoordinator(key, value);
            }

            System.out.printf("[%s] 📝 CA WRITE: %s = %s (timestamp: %d)%n",
                    getName(), key, value, logicalTimestamp);

            metrics.recordWrite(true, System.currentTimeMillis() - startTime);

        } catch (Exception e) {
            metrics.recordWrite(false, System.currentTimeMillis() - startTime);
            throw e;
        }
    }

    /**
     * Read a value from the distributed shared memory
     */
    public String readValue(String key) throws Exception {
        long startTime = System.currentTimeMillis();

        if (isPartitioned()) {
            metrics.recordRead(false, false, System.currentTimeMillis() - startTime);
            throw new Exception("CA system unavailable during partition");
        }

        try {
            // Random pause before operation
            randomPause();

            // In CA system, reads are always consistent and immediately available
            String value = localStorage.get(key);

            System.out.printf("[%s] 📖 CA READ: %s = %s%n", getName(), key, value);

            metrics.recordRead(true, true, System.currentTimeMillis() - startTime);
            return value;
        } catch (Exception e) {
            metrics.recordRead(false, false, System.currentTimeMillis() - startTime);
            throw e;
        }
    }

    /**
     * Handle incoming synchronization messages
     */
    private void handleSyncMessage(DSMSyncMessage message) {
        logicalTimestamp = Math.max(logicalTimestamp, message.getTimestamp()) + 1;

        switch (message.getType()) {
            case COORDINATOR_SYNC:
                handleCoordinatorSync(message);
                break;
            case WRITE_PROPAGATION:
                handleWritePropagation(message);
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
        return CAPType.CA;
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
        return new HashMap<>(localStorage);
    }

    /**
     * Check if this node is the coordinator
     */
    public boolean isCoordinator() {
        return isCentralCoordinator || coordinatorNodeId.equals(getName());
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
     * Handle centralized write operation (coordinator only)
     */
    private void handleCentralizedWrite(String key, String value) throws Exception {
        // Update local storage immediately
        localStorage.put(key, value);

        // Synchronously propagate to all other nodes
        boolean success = propagateWriteSync(key, value, logicalTimestamp);

        if (!success) {
            // Rollback local change if propagation failed
            localStorage.remove(key);
            throw new Exception("Failed to maintain consistency - write rolled back");
        }

        System.out.printf("[%s] ✅ Coordinator processed write: %s = %s%n",
                getName(), key, value);
    }

    /**
     * Forward write request to coordinator
     */
    private void forwardWriteToCoordinator(String key, String value) throws Exception {
        if (coordinatorNodeId.equals(getName())) {
            // This shouldn't happen, but handle gracefully
            handleCentralizedWrite(key, value);
            return;
        }

        DSMSyncMessage writeRequest = new DSMSyncMessage(
                DSMSyncMessage.Type.COORDINATOR_SYNC,
                key,
                value,
                logicalTimestamp,
                getName(),
                DSMSyncMessage.generateMessageId(),
                true); // Requires response

        try {
            sendDSMMessage(writeRequest, coordinatorNodeId);

            // In a full implementation, we would wait for confirmation
            // For simplicity, we assume immediate synchronous propagation
            localStorage.put(key, value);

            System.out.printf("[%s] 📤 Forwarded write to coordinator: %s = %s%n",
                    getName(), key, value);
        } catch (Exception e) {
            throw new Exception("Cannot reach coordinator: " + e.getMessage());
        }
    }

    /**
     * Synchronously propagate write to all nodes
     */
    private boolean propagateWriteSync(String key, String value, long timestamp) {
        int successCount = 0;
        int totalNodes = knownNodes.size() - 1; // Exclude self

        for (String targetNode : knownNodes) {
            if (targetNode.equals(getName())) {
                continue; // Skip self
            }

            DSMSyncMessage syncMessage = new DSMSyncMessage(
                    DSMSyncMessage.Type.COORDINATOR_SYNC,
                    key,
                    value,
                    timestamp,
                    getName());

            try {
                sendDSMMessage(syncMessage, targetNode);
                successCount++;
                System.out.printf("[%s] 📤 Synced write to %s: %s = %s%n",
                        getName(), targetNode, key, value);
            } catch (Exception e) {
                System.err.printf("[%s] ❌ Failed to sync to %s: %s%n",
                        getName(), targetNode, e.getMessage());
                metrics.recordInconsistency("Sync failure to " + targetNode);
            }
        }

        // In CA system, we require ALL nodes to be updated for consistency
        return successCount == totalNodes;
    }

    /**
     * Handle coordinator synchronization messages
     */
    private void handleCoordinatorSync(DSMSyncMessage message) {
        String key = message.getKey();
        String value = message.getValue();

        localStorage.put(key, value);

        System.out.printf("[%s] 🔄 Updated from coordinator: %s = %s%n",
                getName(), key, value);
    }

    /**
     * Handle write propagation (fallback for compatibility)
     */
    private void handleWritePropagation(DSMSyncMessage message) {
        handleCoordinatorSync(message); // Same logic
    }

    /**
     * Determine coordinator node (simple implementation: lexicographically first)
     */
    private String determineCoordinator(Set<String> nodes) {
        return nodes.stream().min(String::compareTo).orElse(getName());
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

    /**
     * Send a DSM sync message to another node
     */
    public void sendDSMMessage(DSMSyncMessage dsmMessage, String targetNodeId) throws UnknownNodeException {
        Message message = new Message();
        message.add("type", dsmMessage.getType().toString());
        message.add("key", dsmMessage.getKey());
        message.add("value", dsmMessage.getValue());
        message.add("timestamp", String.valueOf(dsmMessage.getTimestamp()));
        message.add("originNodeId", dsmMessage.getOriginNodeId());
        message.add("messageId", dsmMessage.getMessageId());
        message.add("requiresResponse", String.valueOf(dsmMessage.requiresResponse()));

        send(message, targetNodeId);
    }
}
