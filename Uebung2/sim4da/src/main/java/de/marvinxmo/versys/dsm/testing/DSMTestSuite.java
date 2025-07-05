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
import de.marvinxmo.versys.dsm.nodes.APNode;
import de.marvinxmo.versys.dsm.nodes.CANode;
import de.marvinxmo.versys.dsm.nodes.CPNode;

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
        double partitionProbability = 0.1; // Probability of partitioning during simulation
        double partitionDurationSec = 4; // Simulate random network failures

        int minPauseMs = 1000; // Minimum pause between read/write operations
        int maxPauseMs = 5000; // Maximum pause between read/write operations

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
            System.err.println("Test failed: " + e.getMessage());
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

        System.out.println("\n" + "=".repeat(60));
        System.out.println("DSM Test Configuration");
        System.out.println("=".repeat(60));

        // Display CAP options
        System.out.println("\nAvailable DSM Types:");
        for (CAPType type : CAPType.values()) {
            System.out.printf("   %s: %s%n", type.name(), type.getDescription());
        }

        // Get CAP type choice
        System.out.print("Choose DSM type (AP/CA/CP) [default: AP]: ");
        String capChoice = scanner.nextLine().toUpperCase().trim();
        if (!capChoice.isEmpty()) {
            try {
                config.capType = CAPType.valueOf(capChoice);
            } catch (IllegalArgumentException e) {
                System.out.println("Invalid choice, using default: AP");
                config.capType = CAPType.AP;
            }
        }

        // Basic simulation parameters
        System.out.println("\nBasic Simulation Parameters:");

        // Get number of nodes
        System.out.printf("Number of nodes [default: %d]: ", config.nodeCount);
        String nodeCountStr = scanner.nextLine().trim();
        if (!nodeCountStr.isEmpty()) {
            try {
                config.nodeCount = Integer.parseInt(nodeCountStr);
                if (config.nodeCount < 1) {
                    System.out.println("Node count must be >= 1, using default");
                    config.nodeCount = 5;
                }
            } catch (NumberFormatException e) {
                System.out.println("Invalid number, using default");
            }
        }

        // Get simulation duration
        System.out.printf("Simulation duration in seconds [default: %d]: ", config.simulationDurationSeconds);
        String durationStr = scanner.nextLine().trim();
        if (!durationStr.isEmpty()) {
            try {
                config.simulationDurationSeconds = Integer.parseInt(durationStr);
                if (config.simulationDurationSeconds < 1) {
                    System.out.println("Duration must be >= 1, using default");
                    config.simulationDurationSeconds = 15;
                }
            } catch (NumberFormatException e) {
                System.out.println("Invalid number, using default");
            }
        }

        // Operation timing parameters
        System.out.println("\n Operation Timing:");

        System.out.printf("Minimum pause between operations (ms) [default: %d]: ", config.minPauseMs);
        String minPauseStr = scanner.nextLine().trim();
        if (!minPauseStr.isEmpty()) {
            try {
                config.minPauseMs = Integer.parseInt(minPauseStr);
                if (config.minPauseMs < 0) {
                    System.out.println("Pause must be >= 0, using default");
                    config.minPauseMs = 100;
                }
            } catch (NumberFormatException e) {
                System.out.println("Invalid number, using default");
            }
        }

        System.out.printf("Maximum pause between operations (ms) [default: %d]: ", config.maxPauseMs);
        String maxPauseStr = scanner.nextLine().trim();
        if (!maxPauseStr.isEmpty()) {
            try {
                config.maxPauseMs = Integer.parseInt(maxPauseStr);
                if (config.maxPauseMs < config.minPauseMs) {
                    System.out.println("Max pause must be >= min pause, using default");
                    config.maxPauseMs = 1000;
                }
            } catch (NumberFormatException e) {
                System.out.println("Invalid number, using default");
            }
        }

        // Network latency simulation
        System.out.println("\n Network Latency Simulation:");
        System.out.printf("Simulate network latency? (y/N) [default: %s]: ", config.simulateNetworkLatency ? "y" : "N");
        String latencyChoice = scanner.nextLine().trim().toLowerCase();
        if (!latencyChoice.isEmpty()) {
            config.simulateNetworkLatency = latencyChoice.startsWith("y");
        }

        if (config.simulateNetworkLatency) {
            System.out.printf("Average latency (ms) [default: %.1f]: ", config.latencyMeanMs);
            String latencyMeanStr = scanner.nextLine().trim();
            if (!latencyMeanStr.isEmpty()) {
                try {
                    config.latencyMeanMs = Double.parseDouble(latencyMeanStr);
                    if (config.latencyMeanMs < 0) {
                        System.out.println("Latency must be >= 0, using default");
                        config.latencyMeanMs = 70;
                    }
                } catch (NumberFormatException e) {
                    System.out.println("Invalid number, using default");
                }
            }

            System.out.printf("Latency standard deviation (ms) [default: %.1f]: ", config.latencyStdMs);
            String latencyStdStr = scanner.nextLine().trim();
            if (!latencyStdStr.isEmpty()) {
                try {
                    config.latencyStdMs = Double.parseDouble(latencyStdStr);
                    if (config.latencyStdMs < 0) {
                        System.out.println("Standard deviation must be >= 0, using default");
                        config.latencyStdMs = 30;
                    }
                } catch (NumberFormatException e) {
                    System.out.println("Invalid number, using default");
                }
            }
        }

        // Network partition simulation
        System.out.println("\n  Network Partition Simulation:");
        System.out.printf("Simulate network partitions? (y/N) [default: %s]: ",
                config.simulateNetworkPartitions ? "y" : "N");
        String partitionChoice = scanner.nextLine().trim().toLowerCase();
        if (!partitionChoice.isEmpty()) {
            config.simulateNetworkPartitions = partitionChoice.startsWith("y");
        }

        if (config.simulateNetworkPartitions) {
            System.out.printf("Partition probability (0.0-1.0) [default: %.2f]: ", config.partitionProbability);
            String partitionProbStr = scanner.nextLine().trim();
            if (!partitionProbStr.isEmpty()) {
                try {
                    config.partitionProbability = Double.parseDouble(partitionProbStr);
                    if (config.partitionProbability < 0.0 || config.partitionProbability > 1.0) {
                        System.out.println("Probability must be between 0.0 and 1.0, using default");
                        config.partitionProbability = 0.9;
                    }
                } catch (NumberFormatException e) {
                    System.out.println("Invalid number, using default");
                }
            }

            System.out.printf("Partition duration (seconds) [default: %.1f]: ", config.partitionDurationSec);
            String partitionDurationStr = scanner.nextLine().trim();
            if (!partitionDurationStr.isEmpty()) {
                try {
                    config.partitionDurationSec = Double.parseDouble(partitionDurationStr);
                    if (config.partitionDurationSec < 0) {
                        System.out.println("Duration must be >= 0, using default");
                        config.partitionDurationSec = 4;
                    }
                } catch (NumberFormatException e) {
                    System.out.println("Invalid number, using default");
                }
            }
        }

        // CAP-specific configuration
        if (config.capType == CAPType.CP) {
            System.out.println("\n CP-Specific Configuration:");
            config.quorumSize = config.nodeCount / 2 + 1; // Majority quorum
            System.out.printf("Quorum size [default: majority (%d)]: ", config.quorumSize);
            String quorumStr = scanner.nextLine().trim();
            if (!quorumStr.isEmpty()) {
                try {
                    config.quorumSize = Integer.parseInt(quorumStr);
                    if (config.quorumSize < 1 || config.quorumSize > config.nodeCount) {
                        System.out.println("Quorum size must be between 1 and node count, using majority");
                        config.quorumSize = config.nodeCount / 2 + 1;
                    }
                } catch (NumberFormatException e) {
                    System.out.println("Invalid number, using majority");
                    config.quorumSize = config.nodeCount / 2 + 1;
                }
            }
        }

        // Display final configuration
        System.out.println("\n" + "=".repeat(60));
        System.out.println(" Final Configuration Summary:");
        System.out.println("=".repeat(60));
        System.out.printf("DSM Type: %s%n", config.capType);
        System.out.printf("Nodes: %d%n", config.nodeCount);
        System.out.printf("Duration: %d seconds%n", config.simulationDurationSeconds);
        System.out.printf("Operation pause: %d-%d ms%n", config.minPauseMs, config.maxPauseMs);
        System.out.printf("Network latency: %s%n",
                config.simulateNetworkLatency ? String.format("%.1f±%.1f ms", config.latencyMeanMs, config.latencyStdMs)
                        : "disabled");
        System.out.printf("Network partitions: %s%n",
                config.simulateNetworkPartitions ? String.format("%.1f%% chance, %.1fs duration",
                        config.partitionProbability * 100, config.partitionDurationSec) : "disabled");
        if (config.capType == CAPType.CP) {
            System.out.printf("Quorum size: %d%n", config.quorumSize);
        }

        System.out.print("\nProceed with this configuration? (Y/n): ");
        String proceed = scanner.nextLine().trim().toLowerCase();
        if (proceed.equals("n") || proceed.equals("no")) {
            System.out.println("Configuration cancelled. Restarting...");
            return getTestConfiguration(scanner);
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

        if (config.capType == CAPType.CA) {
            nodeNames.add("Coordinator"); // Add a coordinator for CP
            config.nodeCount--; // Reduce count for other nodes
        }

        for (int i = 1; i <= config.nodeCount; i++) {
            nodeNames.add("Node" + i);
        }

        // Create nodes
        Map<String, DSMNode> nodes = new HashMap<>();
        ConsistencyMonitor monitor = new ConsistencyMonitor(nodeNames, config.capType);

        for (String nodeName : nodeNames) {

            DSMNode node = null;

            switch (config.capType) {
                case AP:
                    node = new APNode(nodeName);
                    break;
                case CA:
                    node = new CANode(nodeName);
                    break;
                case CP:
                    node = new CPNode(nodeName);
                    break;
                default:
                    node = new APNode(nodeName); // Default to AP
                    break;
            }

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

        if (config.capType.equals(CAPType.CP)) {
            CPNode.approvalsNeeded = config.quorumSize;
        }

        // Start simulation
        Simulator simulator = Simulator.getInstance();

        // Start monitoring
        // monitor.startMonitoring();

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
        // monitor.stopMonitoring();
        simulator.shutdown();

        // Display results
        // displayResults(nodes, monitor);
    }

    /**
     * Display test results and analysis
     */
    // private static void displayResults(Map<String, TestNode> nodes,
    // ConsistencyMonitor monitor) {
    // System.out.println("\n" + "=".repeat(80));
    // System.out.println("📊 TEST RESULTS");
    // System.out.println("=".repeat(80));

    // // Display per-node statistics
    // System.out.println("\n📈 Per-Node Statistics:");
    // for (TestNode node : nodes.values()) {
    // System.out.printf(" %s: %s%n", node.getName(),
    // node.getDSM().getConsistencyMetrics());
    // }

    // // Display consistency analysis
    // System.out.println("\n🔍 Consistency Analysis:");
    // monitor.displayAnalysis();

    // // Display final state comparison
    // System.out.println("\n🗂️ Final State Comparison:");
    // displayStateComparison(nodes);

    // // Display CAP theorem analysis
    // System.out.println("\n📋 CAP Theorem Analysis:");
    // displayCAPAnalysis(nodes.values().iterator().next().getDSM().getCAPType(),
    // monitor);
    // }

    /**
     * Display state comparison across all nodes
     */
    // private static void displayStateComparison(Map<String, TestNode> nodes) {
    // Map<String, Map<String, String>> nodeStates = new HashMap<>();

    // for (TestNode node : nodes.values()) {
    // nodeStates.put(node.getName(), node.getDSM().getLocalState());
    // }

    // // Find all keys across all nodes
    // Set<String> allKeys = new HashSet<>();
    // for (Map<String, String> state : nodeStates.values()) {
    // allKeys.addAll(state.keySet());
    // }

    // boolean isConsistent = true;
    // for (String key : allKeys) {
    // Set<String> values = new HashSet<>();
    // System.out.printf(" Key '%s': ", key);

    // for (String nodeName : nodeStates.keySet()) {
    // String value = nodeStates.get(nodeName).get(key);
    // values.add(value);
    // System.out.printf("%s=%s ", nodeName, value);
    // }

    // if (values.size() > 1) {
    // System.out.print("❌ INCONSISTENT");
    // isConsistent = false;
    // } else {
    // System.out.print("✅ CONSISTENT");
    // }
    // System.out.println();
    // }

    // System.out.printf("\n Overall Consistency: %s%n",
    // isConsistent ? "✅ CONSISTENT" : "❌ INCONSISTENT");
    // }

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
