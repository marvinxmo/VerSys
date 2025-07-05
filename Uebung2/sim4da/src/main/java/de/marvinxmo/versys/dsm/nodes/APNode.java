package de.marvinxmo.versys.dsm.nodes;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import de.marvinxmo.versys.Message;
import de.marvinxmo.versys.dsm.core.DSMNode;
import de.marvinxmo.versys.dsm.monitoring.ConsistencyMetrics;
import de.marvinxmo.versys.utils.ColorPrinter;
import de.marvinxmo.versys.utils.RandomString;

/**
 * AP Node Implementation (Availability + Partition Tolerance)
 * 
 * Features:
 * - Local copies of data on each node
 * - Asynchronous write propagation via gossip protocol
 * - Local reads (always available)
 * - Conflict resolution using "Last Write Wins" with logical timestamps
 * - High availability, eventual consistency
 * - Separate message processing loop that can be disabled during partitioning
 */
public class APNode extends DSMNode {

    private final ConsistencyMetrics metrics;
    private final Map<String, VersionedValue> localStorage;
    private final AtomicBoolean running;

    // Futures to control individual tasks
    private Future<?> messageProcessingTask;
    private Future<?> writeLoopTask;
    private Future<?> readLoopTask;
    private Future<?> partitionTask;

    public APNode(String name) {
        super(name);
        this.localStorage = new ConcurrentHashMap<>();
        this.metrics = new ConsistencyMetrics();
        this.executorService = Executors.newFixedThreadPool(4); // Increased to 4 for message processing
        this.running = new AtomicBoolean(false);
    }

    /**
     * Represents a value with version information for conflict resolution in AP
     */
    private static class VersionedValue {
        final String value;
        final long timestamp;
        final String lastUpdater;

        VersionedValue(String value, long timestamp, String lastUpdater) {
            this.value = value;
            this.timestamp = timestamp;
            this.lastUpdater = lastUpdater;
        }

        @Override
        public String toString() {
            return String.format("VersionedValue[value=%d, timestamp=%d, origin=%s]",
                    value, timestamp, lastUpdater);
        }
    }

    @Override
    public void engage() {
        running.set(true);
        this.messageProcessingEnabled = true;

        // Start all concurrent tasks
        messageProcessingTask = executorService.submit(this::messageProcessingLoop);
        writeLoopTask = executorService.submit(this::randomWriteLoop);
        readLoopTask = executorService.submit(this::randomReadLoop);
        partitionTask = executorService.submit(this::partitionControl);

        System.out.printf("[%s] AP Node started with concurrent operations%n", getName());

        // Main thread just waits for shutdown
        try {
            while (this.isAlive() && !Thread.currentThread().isInterrupted()) {
                sleep(1000); // Check every second
            }
        } finally {
            // System.out.printf("[%s] Main engage loop ending%n", getName());
        }
    }

    /**
     * Handle incoming messages and update local storage
     */
    public void handleIncomingMessage(Message message) {
        String messageType = message.query("type");

        if ("WRITE_PROPAGATION".equals(messageType)) {
            handleWritePropagation(message);
        } else {
            System.out.printf("[%s] Received unsupported message type: %s%n",
                    getName(), messageType);
        }
    }

    /**
     * Handle write propagation messages and update local storage
     */
    private void handleWritePropagation(Message message) {
        try {
            String key = message.query("key");
            String value = message.query("value");
            String timestampStr = message.query("timestamp");
            long timestamp = Long.parseLong(timestampStr);
            String originNodeId = message.query("originNodeId");

            // Update local storage with the new value if it's more recent
            VersionedValue currentValue = localStorage.get(key);

            if (currentValue == null) {
                currentValue = new VersionedValue("empty", 0, "none");
            }

            if (timestamp >= currentValue.timestamp) {
                // // Check for inconsistency - if we're overwriting a different value with
                // similar timestamp
                // if (currentValue.timestamp > 0 && Math.abs(timestamp -
                // currentValue.timestamp) < 1000 &&
                // !currentValue.value.equals(value) &&
                // !currentValue.lastUpdater.equals(originNodeId)) {
                // ColorPrinter.printRed(String.format("[%s] AP INCONSISTENCY DETECTED:
                // Conflict resolution for key '%s' - " +
                // "replacing '%s' (from %s at %d) with '%s' (from %s at %d) due to timestamp
                // ordering",
                // getName(), key, currentValue.value, currentValue.lastUpdater,
                // currentValue.timestamp,
                // value, originNodeId, timestamp));
                // }

                VersionedValue newValue = new VersionedValue(value, timestamp, originNodeId);
                localStorage.put(key, newValue);
                // System.out.printf("[%s] Updated from propagation: %s = %s (from %s)%n",
                // getName(), key, value, originNodeId);
            } else {
                // Inconsistency: receiving older write - indicates network delay or partition
                // healing
                if (currentValue.timestamp - timestamp > 0) {
                    ColorPrinter.printRed(String.format(
                            "[%s] AP INCONSISTENCY DETECTED: Received older write for key '%s' - " +
                                    "got '%s' (timestamp %d) but current is '%s' (timestamp %d). This indicates network partition healing or latency effects.",
                            getName(), key, value, timestamp, currentValue.value, currentValue.timestamp));
                }
            }
        } catch (Exception e) {
            System.err.printf("[%s] Error handling write propagation: %s%n",
                    getName(), e.getMessage());
        }
    }

    public void randomWriteLoop() {

        while (this.isAlive() && !Thread.currentThread().isInterrupted()) {
            try {
                // Random pause before operation
                int pauseMillis = new Random().nextInt(DSMNode.minPauseMs, DSMNode.maxPauseMs);
                sleep(pauseMillis);

                if (Thread.currentThread().isInterrupted() || !this.isAlive()) {
                    break;
                }

                String key = this.getRandomKey();
                VersionedValue prev_value = localStorage.get(key);
                VersionedValue new_value;
                String rstr = new RandomString(8).nextString();

                if (prev_value == null) {
                    new_value = new VersionedValue(rstr, System.currentTimeMillis(), getName());
                } else {
                    new_value = new VersionedValue(rstr, System.currentTimeMillis(), getName());
                }

                // Always perform local write (AP model)
                localStorage.put(key, new_value);
                Boolean broadcasted = false;
                int latency = 0;

                // Try to broadcast (will fail during partition due to disabled message
                // processing)
                if (this.messageProcessingEnabled) {

                    try {
                        Message message = new Message();
                        message.add("type", "WRITE_PROPAGATION");
                        message.add("key", key);
                        message.add("value", String.valueOf(new_value.value));
                        message.add("timestamp", String.valueOf(new_value.timestamp));
                        message.add("originNodeId", getName());

                        latency = this.getLatencyMs();
                        System.out.printf("[%s] Broadcasted write propagation for %s with delay of %d ms %n", getName(),
                                key, latency);
                        sleep(latency);

                        broadcast(message);
                        broadcasted = true;

                    } catch (Exception broadcastError) {
                        System.err.printf("[%s] Broadcast failed: %s%n", getName(), broadcastError.getMessage());
                    }

                } else {

                    ColorPrinter.printRed(String.format(
                            "[%s] AP INCONSISTENCY DETECTED: Wrote to local storage [%s=%s] but cannot broadcast write due to partitioning.",
                            getName(), key, new_value.value));
                    return;

                }
                System.out.printf(
                        "[%s] WRITE: %s = %s (timestamp: %d) [Partitioned: %s] [broadcasted: %s with latency %d] %n",
                        getName(), key, new_value.value, new_value.timestamp, !this.messageProcessingEnabled,
                        broadcasted, latency);

            } catch (Exception e) {
                System.err.printf("[%s] Write operation failed: %s%n", getName(), e.getMessage());
                if (Thread.currentThread().isInterrupted()) {
                    break;
                }
            }
        }

        // System.out.printf("[%s] Write loop ended%n", getName());
    }

    public void randomReadLoop() {

        while (this.isAlive() && !Thread.currentThread().isInterrupted()) {
            try {
                // Random pause before operation
                int pauseMillis = new Random().nextInt(DSMNode.minPauseMs, DSMNode.maxPauseMs);
                sleep(pauseMillis);

                if (Thread.currentThread().isInterrupted() || !this.isAlive()) {
                    break;
                }

                String key = this.getRandomKey();
                VersionedValue read = localStorage.get(key);

                boolean partitioned = !this.messageProcessingEnabled;

                if (read == null) {
                    System.out.printf("[%s] READ: %s not initialized [Partitioned: %s]%n",
                            getName(), key, partitioned);
                    continue;
                }

                // Check for potential stale read during partition
                if (partitioned && read.timestamp > 0) {
                    long timeSinceWrite = System.currentTimeMillis() - read.timestamp;
                    if (timeSinceWrite > 10000) { // 10 seconds old
                        ColorPrinter.printRed(String.format(
                                "[%s] AP INCONSISTENCY DETECTED: Potentially stale read during partition - " +
                                        "key '%s' value '%s' is %d ms old and node is partitioned. May not reflect latest global state.",
                                getName(), key, read.value, timeSinceWrite));
                    }
                }

                System.out.printf("[%s] READ: %s = %s (written by %s at %d) [Partitioned: %s]%n",
                        getName(), key, read.value, read.lastUpdater, read.timestamp, partitioned);

            } catch (Exception e) {
                System.err.printf("[%s] Read operation failed: %s%n", getName(), e.getMessage());
                if (Thread.currentThread().isInterrupted()) {
                    break;
                }
            }
        }

        // System.out.printf("[%s] Read loop ended%n", getName());
    }

    /**
     * Enhanced shutdown method
     */
    @Override
    public void shutdown() {
        System.out.printf("[%s] Shutting down node...%n", getName());

        // Stop message processing
        this.messageProcessingEnabled = false;

        // Cancel all tasks
        if (messageProcessingTask != null)
            messageProcessingTask.cancel(true);
        if (writeLoopTask != null)
            writeLoopTask.cancel(true);
        if (readLoopTask != null)
            readLoopTask.cancel(true);
        if (partitionTask != null)
            partitionTask.cancel(true);

        this.executorService.shutdownNow();

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
            state.put(entry.getKey(), String.valueOf(entry.getValue().value));
        }
        return state;
    }

}