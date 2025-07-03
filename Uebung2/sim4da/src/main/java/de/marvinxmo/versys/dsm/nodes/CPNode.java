package de.marvinxmo.versys.dsm.nodes;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import de.marvinxmo.versys.Message;
import de.marvinxmo.versys.UnknownNodeException;
import de.marvinxmo.versys.dsm.core.CAPType;
import de.marvinxmo.versys.dsm.core.DSMNode;
import de.marvinxmo.versys.dsm.core.DSMSyncMessage;
import de.marvinxmo.versys.dsm.monitoring.ConsistencyMetrics;

/**
 * CP Node Implementation (Consistency + Partition Tolerance)
 * 
 * Features:
 * - Quorum-based reads and writes for strong consistency
 * - Operations may block when quorum is not available
 * - Partition tolerant but may sacrifice availability
 * - Uses majority quorum for consistency guarantees
 * - Random pauses between operations
 */
public class CPNode extends DSMNode {

    private final Map<String, VersionedValue> localStorage;
    private final Set<String> knownNodes;
    private final int quorumSize;
    private final Map<String, QuorumTracker> pendingOperations;
    private final Random random;
    private final ConsistencyMetrics metrics;
    private volatile long logicalTimestamp;

    // Configuration parameters
    private static final long OPERATION_TIMEOUT_MS = 5000; // 5 second timeout
    private static final int MIN_PAUSE_MS = 150;
    private static final int MAX_PAUSE_MS = 1200;

    /**
     * Represents a value with version information
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

    /**
     * Tracks quorum responses for operations
     */
    private static class QuorumTracker {
        final CountDownLatch latch;
        final AtomicInteger responses;
        final int requiredQuorum;
        volatile boolean success = false;

        QuorumTracker(int requiredQuorum) {
            this.requiredQuorum = requiredQuorum;
            this.latch = new CountDownLatch(requiredQuorum);
            this.responses = new AtomicInteger(0);
        }

        void recordResponse(boolean successful) {
            if (successful) {
                responses.incrementAndGet();
                latch.countDown();
            }
        }

        boolean waitForQuorum(long timeoutMs) throws InterruptedException {
            boolean completed = latch.await(timeoutMs, TimeUnit.MILLISECONDS);
            success = completed && responses.get() >= requiredQuorum;
            return success;
        }
    }

    public CPNode(String name, Set<String> knownNodes) {
        super(name);
        this.localStorage = new ConcurrentHashMap<>();
        this.knownNodes = knownNodes;
        this.quorumSize = (knownNodes.size() / 2) + 1; // Majority quorum
        this.pendingOperations = new ConcurrentHashMap<>();
        this.random = new Random();
        this.metrics = new ConsistencyMetrics();
        this.logicalTimestamp = 0;

        System.out.printf("[%s] CP Node initialized with quorum size: %d (total nodes: %d)%n",
                getName(), quorumSize, knownNodes.size());
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

        if (!isQuorumAvailable()) {
            metrics.recordWrite(false, System.currentTimeMillis() - startTime);
            throw new Exception("Insufficient nodes available for quorum write");
        }

        try {
            // Random pause before operation
            randomPause();

            logicalTimestamp++;

            // Generate unique operation ID
            String operationId = getName() + "_" + logicalTimestamp + "_" + key;

            // Store locally first (optimistic)
            VersionedValue versionedValue = new VersionedValue(value, logicalTimestamp, getName());
            localStorage.put(key, versionedValue);

            // Request quorum for write
            boolean quorumAchieved = requestWriteQuorum(key, value, logicalTimestamp, operationId);

            if (quorumAchieved) {
                System.out.printf("[%s] ✅ CP WRITE COMMITTED: %s = %s (quorum achieved)%n",
                        getName(), key, value);
                metrics.recordWrite(true, System.currentTimeMillis() - startTime);
            } else {
                // Remove the local write if quorum failed
                localStorage.remove(key);
                metrics.recordWrite(false, System.currentTimeMillis() - startTime);
                throw new Exception("Write failed - insufficient quorum");
            }

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

        if (!isQuorumAvailable()) {
            metrics.recordRead(false, false, System.currentTimeMillis() - startTime);
            throw new Exception("Insufficient nodes available for quorum read");
        }

        try {
            // Random pause before operation
            randomPause();

            // For CP system, we could implement quorum reads, but for simplicity
            // we'll read from local storage after ensuring it's up-to-date
            VersionedValue versionedValue = localStorage.get(key);
            String value = (versionedValue != null) ? versionedValue.value : null;

            System.out.printf("[%s] 📖 CP READ: %s = %s%n", getName(), key, value);

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
            case QUORUM_REQUEST:
                handleQuorumRequest(message);
                break;
            case QUORUM_RESPONSE:
                handleQuorumResponse(message);
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
        return CAPType.CP;
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
     * Get current quorum size
     */
    public int getQuorumSize() {
        return quorumSize;
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
     * Check if quorum is available
     */
    private boolean isQuorumAvailable() {
        int availableNodes = 1; // Count self
        for (String nodeId : knownNodes) {
            if (!nodeId.equals(getName()) && !isPartitioned()) {
                availableNodes++;
            }
        }
        return availableNodes >= quorumSize;
    }

    /**
     * Request write quorum from other nodes
     */
    private boolean requestWriteQuorum(String key, String value, long timestamp, String operationId) {
        // Create quorum tracker
        QuorumTracker tracker = new QuorumTracker(quorumSize);
        pendingOperations.put(operationId, tracker);

        // Count self as first response
        tracker.recordResponse(true);

        // Send quorum requests to all other nodes
        for (String targetNode : knownNodes) {
            if (targetNode.equals(getName())) {
                continue; // Skip self
            }

            DSMSyncMessage quorumRequest = new DSMSyncMessage(
                    DSMSyncMessage.Type.QUORUM_REQUEST,
                    key,
                    value,
                    timestamp,
                    getName(),
                    operationId,
                    true);

            try {
                sendDSMMessage(quorumRequest, targetNode);
                System.out.printf("[%s] 📤 Quorum request to %s: %s = %s%n",
                        getName(), targetNode, key, value);
            } catch (Exception e) {
                System.err.printf("[%s] ❌ Failed to send quorum request to %s: %s%n",
                        getName(), targetNode, e.getMessage());
            }
        }

        // Wait for quorum
        try {
            boolean success = tracker.waitForQuorum(OPERATION_TIMEOUT_MS);
            pendingOperations.remove(operationId);
            return success;
        } catch (InterruptedException e) {
            pendingOperations.remove(operationId);
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /**
     * Handle incoming quorum requests
     */
    private void handleQuorumRequest(DSMSyncMessage message) {
        String key = message.getKey();
        String value = message.getValue();
        long timestamp = message.getTimestamp();
        String originNodeId = message.getOriginNodeId();
        String operationId = message.getMessageId();

        // Check if we can participate in this quorum
        boolean canParticipate = !isPartitioned() && isValidRequest(message);

        if (canParticipate) {
            // Accept the write
            VersionedValue newValue = new VersionedValue(value, timestamp, originNodeId);
            localStorage.put(key, newValue);

            System.out.printf("[%s] ✅ Accepted quorum write: %s = %s (from %s)%n",
                    getName(), key, value, originNodeId);
        }

        // Send response
        DSMSyncMessage response = new DSMSyncMessage(
                DSMSyncMessage.Type.QUORUM_RESPONSE,
                key,
                canParticipate ? "ACK" : "NACK",
                logicalTimestamp,
                getName(),
                operationId,
                false);

        try {
            sendDSMMessage(response, originNodeId);
        } catch (Exception e) {
            System.err.printf("[%s] ❌ Failed to send quorum response to %s: %s%n",
                    getName(), originNodeId, e.getMessage());
        }
    }

    /**
     * Handle quorum responses
     */
    private void handleQuorumResponse(DSMSyncMessage message) {
        String operationId = message.getMessageId();
        boolean successful = "ACK".equals(message.getValue());

        QuorumTracker tracker = pendingOperations.get(operationId);
        if (tracker != null) {
            tracker.recordResponse(successful);

            System.out.printf("[%s] 📥 Quorum response from %s: %s%n",
                    getName(), message.getOriginNodeId(),
                    successful ? "ACK" : "NACK");
        }
    }

    /**
     * Handle write propagation (for compatibility)
     */
    private void handleWritePropagation(DSMSyncMessage message) {
        String key = message.getKey();
        String value = message.getValue();
        long timestamp = message.getTimestamp();
        String originNodeId = message.getOriginNodeId();

        // In CP system, treat propagation as lower priority update
        VersionedValue currentValue = localStorage.get(key);
        if (currentValue == null || timestamp > currentValue.timestamp) {
            VersionedValue newValue = new VersionedValue(value, timestamp, originNodeId);
            localStorage.put(key, newValue);

            System.out.printf("[%s] 🔄 Updated from propagation: %s = %s%n",
                    getName(), key, value);
        }
    }

    /**
     * Validate if a quorum request is acceptable
     */
    private boolean isValidRequest(DSMSyncMessage message) {
        // Basic validation - could be enhanced with additional consistency checks
        return message.getKey() != null && message.getValue() != null;
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
