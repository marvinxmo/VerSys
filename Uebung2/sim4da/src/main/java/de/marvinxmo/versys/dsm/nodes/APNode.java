package de.marvinxmo.versys.dsm.nodes;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import de.marvinxmo.versys.Message;
import de.marvinxmo.versys.dsm.core.DSMNode;
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
public class APNode extends DSMNode {

    private final ConsistencyMetrics metrics;
    private final Map<String, VersionedValue> localStorage;
    // public final ExecutorService executorService;
    private final AtomicBoolean running;

    public APNode(String name) {
        super(name);
        this.localStorage = new ConcurrentHashMap<>();
        this.metrics = new ConsistencyMetrics();
        this.executorService = Executors.newFixedThreadPool(3);
        this.running = new AtomicBoolean(false);
    }

    /**
     * Represents a value with version information for conflict resolution in AP
     */
    private static class VersionedValue {
        final int value;
        final long timestamp;
        final String lastUpdater;

        VersionedValue(int value, long timestamp, String lastUpdater) {
            this.value = value;
            this.timestamp = timestamp;
            this.lastUpdater = lastUpdater;
        }

        @Override
        public String toString() {
            return String.format("VersionedValue[value=%i, timestamp=%d, origin=%s]",
                    value, timestamp, lastUpdater);
        }
    }

    @Override
    public void engage() {
        running.set(true);

        executorService.submit(this::randomWriteLoop);
        executorService.submit(this::randomReadLoop);
        executorService.submit(this::simulateNetworkPartition);

        System.out.printf("[%s] AP Node started with concurrent operations%n", getName());

        // Continuously process incoming messages
        while (this.isAlive() && !Thread.currentThread().isInterrupted()) {

            // // Wait if partitioned
            // if (this.isPartitioned()) {
            // sleep(500); // Short sleep while partitioned
            // continue; // Go back to start of outer loop
            // }

            // Process messages while not partitioned
            while (!this.isPartitioned() && this.isAlive() && !Thread.currentThread().isInterrupted()) {
                Message message = null;

                try {
                    message = receive();
                } catch (NullPointerException e) {
                    // Handle case where receive returns null due to interruption
                }

                if (Thread.currentThread().isInterrupted() || !this.isAlive()) {
                    break;
                }

                if (message == null) {
                    // Could be due to interruption or no messages available
                    sleep(10); // Small delay to prevent busy waiting
                    if (Thread.currentThread().isInterrupted()) {
                        break;
                    }
                    continue;
                }

                if ("WRITE_PROPAGATION".equals(message.query("type"))) {
                    try {
                        handleWritePropagation(message);
                    } catch (Exception e) {
                        System.err.printf("[%s] Error handling DSM message: %s%n",
                                NodeName(), e.getMessage());
                    }
                } else {
                    // Handle other sync message types if needed
                    System.out.printf("[%s] Received unsupported DSM message type: %s%n",
                            NodeName(), message.query("type"));
                }
            }
        }
    }

    protected void handleWritePropagation(Message message) {
        String key = message.query("key");
        int value = message.queryInteger("value");
        long timestamp = (long) message.queryFloat("timestamp");
        String originNodeId = message.query("originNodeId");

        // Update local storage with the new value if it's more recent
        VersionedValue currentValue = localStorage.get(key);
        if (currentValue == null || timestamp > currentValue.timestamp) {
            VersionedValue newValue = new VersionedValue(value, timestamp, originNodeId);
            localStorage.put(key, newValue);
            System.out.printf("[%s] Received update (%s = %s) from %s%n",
                    getName(), key, value, originNodeId);
        }
    }

    public void randomWriteLoop() {

        while (this.isAlive() && !Thread.currentThread().isInterrupted()) {
            try {
                // Random pause before operation
                int pauseMillis = new Random().nextInt(2000, 7000);
                sleep(pauseMillis);

                if (Thread.currentThread().isInterrupted() || !this.isAlive()) {
                    break;
                }

                String key = this.getRandomKey();
                VersionedValue prev_value = localStorage.get(key);
                VersionedValue new_value;

                if (prev_value == null) {
                    new_value = new VersionedValue(1, System.currentTimeMillis(), getName());
                } else {
                    new_value = new VersionedValue(prev_value.value + 1, System.currentTimeMillis(), getName());
                }

                if (Thread.currentThread().isInterrupted() || !this.isAlive()) {
                    break;
                }

                localStorage.put(key, new_value);
                int latency = getLatencyMs();

                System.out.printf("[%s] WRITE: %s = %s (timestamp: %d, latency: %d)%n",
                        getName(), key, new_value.value, new_value.timestamp, latency);

                sleep(latency);

                if (Thread.currentThread().isInterrupted() || !this.isAlive()) {
                    break;
                }

                if (!this.isPartitioned()) {
                    // Simple broadcast without DSMSyncMessage
                    try {
                        Message message = new Message();
                        message.add("type", "WRITE_PROPAGATION");
                        message.add("key", key);
                        message.add("value", String.valueOf(new_value.value));
                        message.add("timestamp", String.valueOf(new_value.timestamp));
                        message.add("originNodeId", getName());

                        broadcast(message);

                    } catch (Exception broadcastError) {
                        System.err.printf("[%s] Broadcast failed: %s%n", getName(), broadcastError.getMessage());
                    }
                }
                // metrics.recordWrite(true, System.currentTimeMillis() - startTime);

            } catch (Exception e) {
                // metrics.recordWrite(false, System.currentTimeMillis() - startTime);
                System.err.printf("[%s] Write failed: %s%n", getName(), e.getMessage());
            }
        }
    }

    public void randomReadLoop() {

        while (this.isAlive() && !Thread.currentThread().isInterrupted()) {
            try {
                // Random pause before operation
                int pauseMillis = new Random().nextInt(2000, 7000);
                sleep(pauseMillis);

                if (Thread.currentThread().isInterrupted() || !this.isAlive()) {
                    break;
                }

                String key = this.getRandomKey();
                VersionedValue read = localStorage.get(key);

                if (read == null) {
                    System.out.printf("[%s] READ: %s not initialized %n", getName(), key);
                    continue;
                }

                System.out.printf("[%s] READ: %s = %s (written by Node %s at %d)%n",
                        getName(), key, read.value, read.lastUpdater, read.timestamp);

                // metrics.recordWrite(true, System.currentTimeMillis() - startTime);

            } catch (Exception e) {
                // metrics.recordWrite(false, System.currentTimeMillis() - startTime);
                System.err.printf("[%s] Read failed: %s%n", getName(), e.getMessage());
            }
        }
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
