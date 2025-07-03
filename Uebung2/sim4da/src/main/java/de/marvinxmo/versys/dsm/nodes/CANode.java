package de.marvinxmo.versys.dsm.nodes;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import de.marvinxmo.versys.Message;
import de.marvinxmo.versys.dsm.core.DSMNode;
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
public class CANode extends DSMNode {

    // This is the DSMs data storage
    public Map<String, VersionedValue> storage;
    public Message lastCoordinatorResponse;

    private final AtomicBoolean running;

    // Futures to control individual tasks
    private Future<?> messageProcessingTask;
    private Future<?> writeLoopTask;
    private Future<?> readLoopTask;
    private Future<?> partitionTask;

    public CANode(String name) {
        super(name);
        this.executorService = Executors.newFixedThreadPool(4); // Increased to 4 for message processing
        this.running = new AtomicBoolean(false);
        this.storage = new ConcurrentHashMap<>();

        if (name.equals("Coordinator")) {
            for (String key : KEYS_FOR_DSM) {
                this.storage.put(key, new VersionedValue("empty", 0, "none"));
            }
        }
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
        partitionTask = executorService.submit(this::partitionControlLoop);

        System.out.printf("[%s] CA Node started with concurrent operations%n", getName());

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

        if ("COORDINATER_READ_RESPONSE".equals(messageType)) {
            this.lastCoordinatorResponse = message;
            return;
        }

        if (this.getName() != "Coordinator") {
            System.out.printf("[%s] Ignoring message - only Coordinator should get read/write requests %n", getName());
            return;
        }

        if ("COORDINATER_WRITE_REQUEST".equals(messageType)) {
            handleCoordinatorWrite(message);
            return;
        }

        if ("COORDINATOR_READ_REQUEST".equals(messageType)) {
            handleCoordinatorRead(message);
            return;
        }

        System.out.printf("[%s] Received unsupported message type: %s%n",
                getName(), messageType);

    }

    /**
     * Handle write propagation messages and update local storage
     */
    private void handleCoordinatorWrite(Message message) {

        try {
            String key = message.query("key");
            String value = message.query("value");
            String timestampStr = message.query("timestamp");
            long timestamp = Long.parseLong(timestampStr);
            String originNodeId = message.query("originNodeId");

            // Update local storage with the new value if it's more recent
            VersionedValue currentValue = storage.get(key);

            if (currentValue == null) {
                currentValue = new VersionedValue("x", 0, "none");
            }

            if (timestamp >= currentValue.timestamp) {
                VersionedValue newValue = new VersionedValue(value, timestamp, originNodeId);
                storage.put(key, newValue);
                System.out.printf("[%s] Coordinator updated Storage: %s = %s (from %s)%n",
                        getName(), key, value, originNodeId);
            } else {
                System.out.printf("[%s] Received update ignored: %s = %s (timestamp %d < %d)%n",
                        getName(), key, value, timestamp, currentValue.timestamp);
            }
        } catch (

        Exception e) {
            System.err.printf("[%s] Error handling request: %s%n",
                    getName(), e.getMessage());
        }
    }

    /**
     * Handle read requests from the coordinator
     */
    private void handleCoordinatorRead(Message message) {
        try {
            String key = message.query("key");
            VersionedValue value = storage.get(key);

            if (value != null) {
                Message response = new Message();
                response.add("type", "COORDINATOR_READ_RESPONSE");
                response.add("key", key);
                response.add("timestamp", String.valueOf(value.timestamp));
                response.add("originNodeId", getName());

                // Send the response back to the coordinator
                send(response, message.queryHeader("originNodeId"));
            }
        } catch (Exception e) {
            System.err.printf("[%s] Error handling read request: %s%n",
                    getName(), e.getMessage());
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
                VersionedValue prev_value = storage.get(key);
                VersionedValue new_value;
                String rstr = new RandomString(8).nextString();

                if (prev_value == null) {
                    new_value = new VersionedValue(rstr, System.currentTimeMillis(), getName());
                } else {
                    new_value = new VersionedValue(rstr, System.currentTimeMillis(), getName());
                }

                // Try to broadcast (will fail during partition due to disabled message
                // processing)
                if (this.messageProcessingEnabled) {

                    try {
                        Message message = new Message();
                        message.add("type", "COORDINATER_WRITE_REQUEST");
                        message.add("key", key);
                        message.add("value", String.valueOf(new_value.value));
                        message.add("timestamp", String.valueOf(new_value.timestamp));
                        message.add("originNodeId", getName());

                        int latency = this.getLatencyMs();
                        System.out.printf(
                                "[%s] Send WRITE_REQUEST: %s = %s (timestamp: %d) [Partitioned: %s] [latency: %d] %n",
                                getName(), key, new_value.value, new_value.timestamp, this.messageProcessingEnabled,
                                latency);
                        sleep(latency);

                        send(message, "Coordinator");

                    } catch (Exception sendError) {
                        System.err.printf("[%s] Send failed: %s%n", getName(), sendError.getMessage());
                    }
                } else {
                    System.out.printf("[%s] Skipping send - node is partitioned%n", getName());
                }

            } catch (Exception e) {
                System.err.printf("[%s] Write request failed: %s%n", getName(), e.getMessage());
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
                int pauseMillis = new Random().nextInt(2000, 7000);
                sleep(pauseMillis);

                if (Thread.currentThread().isInterrupted() || !this.isAlive()) {
                    break;
                }

                String key = this.getRandomKey();

                Message message = new Message();
                message.add("type", "COORDINATER_READ_REQUEST");
                message.add("key", key);
                message.add("timestamp", String.valueOf(System.currentTimeMillis()));
                message.add("originNodeId", getName());

                boolean partitioned = !this.messageProcessingEnabled;

                if (partitioned) {
                    System.out.printf("[%s] Cant send read requests atm - node is partitioned%n", getName());
                    continue;
                }

                try {
                    send(message, "Coordinator");
                } catch (Exception sendError) {
                    System.err.printf("[%s] Read request send failed: %s%n", getName(), sendError.getMessage());
                }

                while (this.lastCoordinatorResponse == null) {
                    sleep(100); // Wait for response
                    if (Thread.currentThread().isInterrupted()) {
                        return; // Exit if interrupted
                    }
                }

                Message response = this.lastCoordinatorResponse;

                String value = response.query("value");
                long timestamp = Long.parseLong(response.query("timestamp"));
                String lastUpdater = response.query("originNodeId");

                System.out.printf("[%s] Sucessfully requested READ: %s = %s (written by %s at %d) [Partitioned: %s]%n",
                        getName(), key, value, lastUpdater, timestamp, partitioned);

                this.lastCoordinatorResponse = null; // Reset for next read

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

        // Call parent shutdown
        super.shutdown();
    }

}