package de.marvinxmo.versys.dsm.testing;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;

import de.marvinxmo.versys.Simulator;
import de.marvinxmo.versys.dsm.core.CAPType;
import de.marvinxmo.versys.dsm.core.DSMNode;
import de.marvinxmo.versys.dsm.monitoring.ConsistencyMonitor;
import de.marvinxmo.versys.dsm.nodes.APNode3;

/**
 * Comprehensive test application for all three DSM implementations
 * Allows user to choose between AP, CA, and CP DSM configurations
 * Includes consistency monitoring and visualization
 */
public class DSMTestSuite {

    /**
     * Configuration for the DSM test
     */
    public static class TestConfiguration {

        CAPType capType = CAPType.AP;
        int nodeCount = 5;
        int simulationDurationSeconds = 15;

        boolean simulateNetworkLatency = true;
        double latencyMeanMs = 70;
        double latencyStdMs = 30;

        boolean simulateNetworkPartitions = true;
        double partitionProbability = 0.9; // Probability of partitioning during simulation
        double partitionDurationSec = 4; // Simulate random network failures

        int minPauseMs = 100; // Minimum pause between read/write operations
        int maxPauseMs = 1000; // Maximum pause between read/write operations

        // For AP

        // For CA

        // For CP (Quorum)
        int quorumSize = nodeCount / 2; // Default to majority

    }

    public static void main(String[] args) {
        System.out.println("=".repeat(80));
        System.out.println("Distributed Shared Memory (DSM) Test Suite");
        System.out.println("   Testing CAP Theorem Implementations: AP, CA, CP");
        System.out.println("=".repeat(80));

        Scanner scanner = new Scanner(System.in);

        try {
            // Get configuration from user
            TestConfiguration config = getTestConfiguration(scanner);

            // Run the test
            runDSMTest(config);

        } catch (Exception e) {
            System.err.println("❌ Test failed: " + e.getMessage());
            e.printStackTrace();
        } finally {
            scanner.close();
        }
    }

    /**
     * Get test configuration from user input
     */
    private static TestConfiguration getTestConfiguration(Scanner scanner) {
        TestConfiguration config = new TestConfiguration();

        // Display CAP options
        System.out.println("\nAvailable DSM Types:");
        for (CAPType type : CAPType.values()) {
            System.out.printf("   %s: %s%n", type.name(), type.getDescription());
        }

        // Get CAP type choice
        System.out.print("Choose DSM type (AP/CA/CP): ");
        String capChoice = scanner.nextLine().toUpperCase();
        try {
            config.capType = CAPType.valueOf(capChoice);
        } catch (IllegalArgumentException e) {
            System.out.println(" Invalid choice, defaulting to AP");
            config.capType = CAPType.AP;
        }

        // Get number of nodes
        System.out.printf("Number of nodes (default %d): ", config.nodeCount);
        String nodeCountStr = scanner.nextLine();
        config.nodeCount = nodeCountStr.isEmpty() ? config.nodeCount : Integer.parseInt(nodeCountStr);

        // Get simulation duration
        System.out.printf("Simulation duration in seconds (default %d): ", config.simulationDurationSeconds);
        String durationStr = scanner.nextLine();
        config.simulationDurationSeconds = durationStr.isEmpty() ? 30 : Integer.parseInt(durationStr);

        // Ask about network partition simulation
        System.out.print("Simulate network partitions? (y/N): ");
        config.simulateNetworkPartitions = scanner.nextLine().toLowerCase().startsWith("y");

        if (config.capType == CAPType.CP) {
            config.quorumSize = config.nodeCount / 2;
            System.out.printf("Quorum size (default: majority (%d)): ", config.quorumSize);
            String quorumStr = scanner.nextLine();
            if (!quorumStr.isEmpty()) {
                config.quorumSize = Integer.parseInt(quorumStr);
            }
        }

        return config;
    }

    /**
     * Run the DSM test with the given configuration
     */
    private static void runDSMTest(TestConfiguration config) {
        System.out.println("\n" + "=".repeat(80));
        System.out.printf("Starting %s DSM Test\n", config.capType);
        System.out.printf("   Nodes: %d | Duration: %ds\n",
                config.nodeCount, config.simulationDurationSeconds);
        System.out.println("=".repeat(80));

        // Create node names
        Set<String> nodeNames = new HashSet<>();
        for (int i = 1; i <= config.nodeCount; i++) {
            nodeNames.add("Node" + i);
        }

        // Create nodes
        Map<String, DSMNode> nodes = new HashMap<>();
        ConsistencyMonitor monitor = new ConsistencyMonitor(nodeNames, config.capType);

        for (String nodeName : nodeNames) {
            DSMNode node = new APNode3(nodeName);
            nodes.put(nodeName, node);
        }

        DSMNode.simulationDurationSec = config.simulationDurationSeconds;
        DSMNode.simulateNetworkLatency = config.simulateNetworkLatency;
        DSMNode.latencyMeanMs = config.latencyMeanMs;
        DSMNode.latencyStdMs = config.latencyStdMs;
        DSMNode.simulateNetworkPartitions = config.simulateNetworkPartitions;
        DSMNode.partitionProbability = config.partitionProbability;
        DSMNode.partitionDurationSec = config.partitionDurationSec;
        DSMNode.minPauseMs = config.minPauseMs;
        DSMNode.maxPauseMs = config.maxPauseMs;

        // Start simulation
        Simulator simulator = Simulator.getInstance();

        // Start monitoring
        monitor.startMonitoring();

        System.out.println("Starting simulation...");

        // Start the simulation in a background thread
        Thread simulationThread = new Thread(() -> {
            simulator.simulate(config.simulationDurationSeconds);
        });
        simulationThread.start();

        try {
            Thread.sleep(config.simulationDurationSeconds * 1000);
        } catch (InterruptedException e) {
            System.out.println("Simulation interrupted.1");
            Thread.currentThread().interrupt();
        }

        for (DSMNode node : nodes.values()) {
            // node.executorService.shutdownNow(); // Stop all executor services
            node.isAlive = false;
            node.shutdown(); // Ensure node is properly shut down
        }

        // Wait for simulation to complete
        try {
            simulationThread.join();
        } catch (InterruptedException e) {
            System.out.println("Simulation interrupted.2");
            Thread.currentThread().interrupt();

        }

        // Stop monitoring
        monitor.stopMonitoring();
        simulator.shutdown();

        // Display results
        // displayResults(nodes, monitor);
    }

    /**
     * Display test results and analysis
     */
    private static void displayResults(Map<String, TestNode> nodes, ConsistencyMonitor monitor) {
        System.out.println("\n" + "=".repeat(80));
        System.out.println("📊 TEST RESULTS");
        System.out.println("=".repeat(80));

        // Display per-node statistics
        System.out.println("\n📈 Per-Node Statistics:");
        for (TestNode node : nodes.values()) {
            System.out.printf("  %s: %s%n", node.getName(),
                    node.getDSM().getConsistencyMetrics());
        }

        // Display consistency analysis
        System.out.println("\n🔍 Consistency Analysis:");
        monitor.displayAnalysis();

        // Display final state comparison
        System.out.println("\n🗂️  Final State Comparison:");
        displayStateComparison(nodes);

        // Display CAP theorem analysis
        System.out.println("\n📋 CAP Theorem Analysis:");
        displayCAPAnalysis(nodes.values().iterator().next().getDSM().getCAPType(), monitor);
    }

    /**
     * Display state comparison across all nodes
     */
    private static void displayStateComparison(Map<String, TestNode> nodes) {
        Map<String, Map<String, String>> nodeStates = new HashMap<>();

        for (TestNode node : nodes.values()) {
            nodeStates.put(node.getName(), node.getDSM().getLocalState());
        }

        // Find all keys across all nodes
        Set<String> allKeys = new HashSet<>();
        for (Map<String, String> state : nodeStates.values()) {
            allKeys.addAll(state.keySet());
        }

        boolean isConsistent = true;
        for (String key : allKeys) {
            Set<String> values = new HashSet<>();
            System.out.printf("  Key '%s': ", key);

            for (String nodeName : nodeStates.keySet()) {
                String value = nodeStates.get(nodeName).get(key);
                values.add(value);
                System.out.printf("%s=%s ", nodeName, value);
            }

            if (values.size() > 1) {
                System.out.print("❌ INCONSISTENT");
                isConsistent = false;
            } else {
                System.out.print("✅ CONSISTENT");
            }
            System.out.println();
        }

        System.out.printf("\n  Overall Consistency: %s%n",
                isConsistent ? "✅ CONSISTENT" : "❌ INCONSISTENT");
    }

    private static void displayCAPAnalysis(CAPType capType, ConsistencyMonitor monitor) {
        switch (capType) {
            case AP:
                System.out.println("  📍 AP System (Availability + Partition Tolerance):");
                System.out.println("    ✅ High availability - operations continue during partitions");
                System.out.println("    ✅ Partition tolerant - system remains operational");
                System.out.println("    ⚠️  Eventual consistency - temporary inconsistencies expected");
                break;

            case CA:
                System.out.println("  📍 CA System (Consistency + Availability):");
                System.out.println("    ✅ Strong consistency - all operations maintain consistency");
                System.out.println("    ✅ High availability - fast operations when network is stable");
                System.out.println("    ❌ No partition tolerance - fails during network splits");
                break;

            case CP:
                System.out.println("  📍 CP System (Consistency + Partition Tolerance):");
                System.out.println("    ✅ Strong consistency - quorum ensures data consistency");
                System.out.println("    ✅ Partition tolerant - continues with majority quorum");
                System.out.println("    ⚠️  Limited availability - may block when quorum unavailable");
                break;
        }
    }

}
