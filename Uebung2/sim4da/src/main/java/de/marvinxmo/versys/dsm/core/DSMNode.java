package de.marvinxmo.versys.dsm.core;

import java.util.Random;
import java.util.concurrent.ExecutorService;

import de.marvinxmo.versys.Message;
import de.marvinxmo.versys.Node;

/**
 * Enhanced base class for nodes that use Distributed Shared Memory
 * Provides better abstraction and monitoring capabilities
 */
public abstract class DSMNode extends Node {

    public static int simulationDurationSec; // Simulation duration in seconds

    public static boolean simulateNetworkLatency;
    public static double latencyMeanMs;
    public static double latencyStdMs;

    public static boolean simulateNetworkPartitions;
    public static double partitionProbability;
    public static double partitionDurationSec;

    public static int minPauseMs; // Minimum pause between read/write operations
    public static int maxPauseMs; // Maximum pause between read/write operations

    public boolean isAlive = true;
    private Random random = new Random();
    public boolean isPartitioned = false;
    public ExecutorService executorService;
    public boolean messageProcessingEnabled = true;

    // Configuration parameters

    // Random read/write actions are generated for these keys
    public final String[] KEYS_FOR_DSM = {
            "giraffe",
            "zebra",
            "lion",
            "elephant",
            "monkey",
            "koala"

    };

    /**
     * Represents a value with version information for conflict resolution in AP
     */
    public class VersionedValue {
        public final String value;
        public final long timestamp;
        public final String lastUpdater;

        public VersionedValue(String value, long timestamp, String lastUpdater) {
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

    public DSMNode(String name) {
        super(name);
    }

    public String getName() {
        return NodeName();
    }

    public String getRandomKey() {
        return KEYS_FOR_DSM[random.nextInt(KEYS_FOR_DSM.length)];
    }

    public int getLatencyMs() {
        if (simulateNetworkLatency) {
            // Simulate network latency using a normal distribution
            return (int) Math.max(0, Math.round(
                    random.nextGaussian() * latencyStdMs + latencyMeanMs));
        } else {
            return 0; // No latency simulation
        }
    }

    public void messageProcessingLoop() {

        while (this.isAlive() && !Thread.currentThread().isInterrupted()) {
            try {
                // Skip message processing if disabled (during partition)
                if (!this.messageProcessingEnabled) {
                    sleep(100); // Short wait when disabled
                    if (!this.getName().equals("Coordinator")) {
                        // If not Coordinator, continue to next iteration
                        // Coordinator needs to receive to detect inconsistencies
                        continue;
                    }
                }

                Message message = null;
                try {
                    message = receive();
                } catch (Exception e) {
                    // Handle any receive exceptions
                    if (Thread.currentThread().isInterrupted()) {
                        break;
                    }
                    continue;
                }

                if (Thread.currentThread().isInterrupted() || !this.isAlive()) {
                    break;
                }

                if (!this.messageProcessingEnabled && !this.getName().equals("Coordinator")) {
                    continue;
                }

                if (message == null) {
                    sleep(10); // Small delay to prevent busy waiting
                    continue;
                }

                // Process the message
                handleIncomingMessage(message);

            } catch (Exception e) {
                System.err.printf("[%s] Error in message processing loop: %s%n",
                        getName(), e.getMessage());
                if (Thread.currentThread().isInterrupted()) {
                    break;
                }
            }
        }

        // System.out.printf("[%s] Message processing loop ended%n", getName());
    }

    /**
     * Control loop for network partitioning
     * This replaces the simulateNetworkPartition method
     */
    public void partitionControl() {

        try {

            boolean shouldBePartitioned = simulateNetworkPartitions && Math.random() < partitionProbability;

            if (!shouldBePartitioned) {
                return;
            }

            // Wait for random interval before considering partition
            int waitTime = new Random().nextInt(1000,
                    (int) (DSMNode.simulationDurationSec - DSMNode.partitionDurationSec - 1) * 1000);
            try {
                Thread.sleep(waitTime);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            if (!this.isAlive() || Thread.currentThread().isInterrupted()) {
                return;
            }

            triggerNetworkPartition();

        } catch (Exception e) {
            System.err.printf("[%s] Error in partition control loop: %s%n",
                    getName(), e.getMessage());
            if (Thread.currentThread().isInterrupted()) {
                return;
            }
        }
    }

    // System.out.printf("[%s] Partition control loop ended%n", getName());

    /**
     * Trigger a network partition by disabling message processing and network
     * connection
     */
    public void triggerNetworkPartition() {
        System.out.printf("[%s] *** NETWORK PARTITION STARTED ***%n", getName());

        // Disable message processing
        this.messageProcessingEnabled = false;

        // Calculate partition duration
        int partitionDurationMs = (int) (partitionDurationSec * 1000);

        try {
            sleep(partitionDurationMs);
            endNetworkPartition();
        } catch (Exception e) {
            System.err.printf("[%s] Error ending partition: %s%n", getName(), e.getMessage());
        }
    };

    /**
     * End network partition by re-enabling message processing
     */
    public void endNetworkPartition() {
        System.out.printf("[%s] *** NETWORK PARTITION ENDED ***%n", getName());

        // Re-enable message processing
        this.messageProcessingEnabled = true;
    }

    public abstract void handleIncomingMessage(Message message);

    public abstract void randomWriteLoop();

    public abstract void randomReadLoop();

    public abstract void shutdown();

    public boolean isPartitioned() {
        return isPartitioned;
    }

    public boolean isAlive() {
        return isAlive;
    }
}
