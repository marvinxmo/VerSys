package de.marvinxmo.versys.dsm.core;

import java.util.Random;
import java.util.concurrent.ExecutorService;

import de.marvinxmo.versys.Node;

/**
 * Enhanced base class for nodes that use Distributed Shared Memory
 * Provides better abstraction and monitoring capabilities
 */
public abstract class DSMNode extends Node {

    public static int simulationDurationSec = 15; // Simulation duration in seconds

    public static boolean simulateNetworkLatency = true;
    public static double latencyMeanMs = 70;
    public static double latencyStdMs = 30;

    public static boolean simulateNetworkPartitions = false;
    public static double partitionProbability = 0.99;
    public static double partitionDurationSec = 4;

    public static int minPauseMs = 100; // Minimum pause between read/write operations
    public static int maxPauseMs = 1000; // Maximum pause between read/write operations

    public boolean isAlive = true;
    private Random random = new Random();
    public boolean isPartitioned = false;
    public ExecutorService executorService;

    // Configuration parameters

    // Random read/write actions are generated for these keys
    public final String[] KEYS_FOR_DSM = {
            "giraffe", "zebra", "lion", "elephant", "monkey", "koala"
    };

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

    public void simulateNetworkPartition() {
        System.out.println("Simulating network partition function called");
        if (simulateNetworkPartitions && random.nextDouble() < partitionProbability) {
            // wait until random time in simulation
            int waitTime = random.nextInt(0, (int) (simulationDurationSec * 1000 - 1000));
            System.out.println("partition wait time: " + waitTime + " ms");
            sleep(waitTime);
            this.isPartitioned = true;
            System.out.printf("[%s] NNN Network partition simulated%n", NodeName());
            // Simulate partition duration
            sleep((int) (partitionDurationSec * 1000));
            this.isPartitioned = false;
            System.out.printf("[%s] Network partition resolved%n", NodeName());
        }
    }

    public abstract void randomWriteLoop();

    public abstract void randomReadLoop();

    public boolean isPartitioned() {
        return isPartitioned;
    }

    public boolean isAlive() {
        return isAlive;
    }

    public void shutdown() {
        if (executorService != null && !executorService.isShutdown()) {
            executorService.shutdownNow(); // This will interrupt all running tasks
            try {
                if (!executorService.awaitTermination(2, java.util.concurrent.TimeUnit.SECONDS)) {
                    System.err.printf("[%s] ExecutorService did not terminate gracefully%n", getName());
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                System.err.printf("[%s] Shutdown interrupted%n", getName());
            }
        }
        System.out.printf("[%s] Node shutdown completed%n", getName());
    }
}
