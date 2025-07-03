package de.marvinxmo.versys.dsm.testing;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;
import java.util.Set;

import de.marvinxmo.versys.Simulator;
import de.marvinxmo.versys.dsm.core.CAPType;
import de.marvinxmo.versys.dsm.monitoring.ConsistencyMonitor;
import de.marvinxmo.versys.dsm.nodes.APNode;
import de.marvinxmo.versys.dsm.nodes.CANode;
import de.marvinxmo.versys.dsm.nodes.CPNode;

/**
 * New simplified test suite using CAP-specific node types
 * Each CAP variation has its own Node type that manages read/write operations
 */
public class CAPNodeTestSuite {

    private static final int DEFAULT_NODE_COUNT = 5;
    private static final int DEFAULT_OPERATIONS_PER_NODE = 10;

    public static void main(String[] args) {
        System.out.println("=".repeat(80));
        System.out.println("🚀 CAP Node Test Suite - Direct Node Implementation");
        System.out.println("   Testing CAP Theorem with Node-based Architecture");
        System.out.println("=".repeat(80));

        Scanner scanner = new Scanner(System.in);

        try {
            // Get configuration from user
            TestConfiguration config = getTestConfiguration(scanner);

            // Run the test
            runCAPNodeTest(config);

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
        System.out.println("\n📋 Available CAP Node Types:");
        for (CAPType type : CAPType.values()) {
            System.out.printf("   %s: %s%n", type.name(), type.getDescription());
        }

        // Get CAP type choice
        System.out.print("\n🔧 Choose CAP Node type (AP/CA/CP): ");
        String capChoice = scanner.nextLine().toUpperCase();
        try {
            config.capType = CAPType.valueOf(capChoice);
        } catch (IllegalArgumentException e) {
            System.out.println("⚠️  Invalid choice, defaulting to AP");
            config.capType = CAPType.AP;
        }

        // Get number of nodes
        System.out.printf("🔧 Number of nodes (default %d): ", DEFAULT_NODE_COUNT);
        String nodeCountStr = scanner.nextLine();
        config.nodeCount = nodeCountStr.isEmpty() ? DEFAULT_NODE_COUNT : Integer.parseInt(nodeCountStr);

        // Get operations per node
        System.out.printf("🔧 Operations per node (default %d): ", DEFAULT_OPERATIONS_PER_NODE);
        String opsStr = scanner.nextLine();
        config.operationsPerNode = opsStr.isEmpty() ? DEFAULT_OPERATIONS_PER_NODE : Integer.parseInt(opsStr);

        // Get simulation duration
        System.out.print("🔧 Simulation duration in seconds (default 20): ");
        String durationStr = scanner.nextLine();
        config.simulationDurationSeconds = durationStr.isEmpty() ? 20 : Integer.parseInt(durationStr);

        // Ask about network partition simulation
        System.out.print("🔧 Simulate network partitions? (y/N): ");
        config.simulatePartitions = scanner.nextLine().toLowerCase().startsWith("y");

        return config;
    }

    /**
     * Run the CAP node test with the given configuration
     */
    private static void runCAPNodeTest(TestConfiguration config) {
        System.out.println("\n" + "=".repeat(80));
        System.out.printf("🚀 Starting %s CAP Node Test\n", config.capType);
        System.out.printf("   Nodes: %d | Operations per node: %d | Duration: %ds\n",
                config.nodeCount, config.operationsPerNode, config.simulationDurationSeconds);
        System.out.println("=".repeat(80));

        // Create node names
        Set<String> nodeNames = new HashSet<>();
        for (int i = 1; i <= config.nodeCount; i++) {
            nodeNames.add("Node" + i);
        }

        // Create CAP-specific nodes
        Map<String, CAPNodeBase> nodes = createCAPNodes(config.capType, nodeNames);
        ConsistencyMonitor monitor = new ConsistencyMonitor(nodeNames, config.capType);

        // Start monitoring
        monitor.startMonitoring();

        // Create test workers for each node
        for (CAPNodeBase node : nodes.values()) {
            TestWorker worker = new TestWorker(node, config, monitor);
            new Thread(worker).start();
        }

        // Start simulation
        Simulator simulator = Simulator.getInstance();

        System.out.println("📡 Starting simulation...");

        // Start the simulation in a background thread
        Thread simulationThread = new Thread(() -> {
            simulator.simulate(config.simulationDurationSeconds);
        });
        simulationThread.start();

        // Simulate network partitions if requested
        if (config.simulatePartitions) {
            simulateNetworkPartitions(nodes, config.simulationDurationSeconds);
        }

        // Wait for simulation to complete
        try {
            simulationThread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Stop monitoring
        monitor.stopMonitoring();
        simulator.shutdown();

        // Display results
        displayResults(nodes, monitor);
    }

    /**
     * Create CAP-specific nodes based on the chosen type
     */
    private static Map<String, CAPNodeBase> createCAPNodes(CAPType capType, Set<String> nodeNames) {
        Map<String, CAPNodeBase> nodes = new HashMap<>();

        boolean firstNode = true;
        for (String nodeName : nodeNames) {
            CAPNodeBase node = null;

            switch (capType) {
                case AP:
                    node = new CAPNodeBase(new APNode(nodeName, nodeNames));
                    break;
                case CA:
                    // First node becomes coordinator
                    node = new CAPNodeBase(new CANode(nodeName, nodeNames, firstNode));
                    break;
                case CP:
                    node = new CAPNodeBase(new CPNode(nodeName, nodeNames));
                    break;
            }

            if (node != null) {
                nodes.put(nodeName, node);
            }
            firstNode = false;
        }

        return nodes;
    }

    /**
     * Wrapper class to provide uniform interface for different CAP node types
     */
    private static class CAPNodeBase {
        private final Object node;

        public CAPNodeBase(Object node) {
            this.node = node;
        }

        public void writeValue(String key, String value) throws Exception {
            if (node instanceof APNode) {
                ((APNode) node).writeValue(key, value);
            } else if (node instanceof CANode) {
                ((CANode) node).writeValue(key, value);
            } else if (node instanceof CPNode) {
                ((CPNode) node).writeValue(key, value);
            }
        }

        public String readValue(String key) throws Exception {
            if (node instanceof APNode) {
                return ((APNode) node).readValue(key);
            } else if (node instanceof CANode) {
                return ((CANode) node).readValue(key);
            } else if (node instanceof CPNode) {
                return ((CPNode) node).readValue(key);
            }
            return null;
        }

        public String getName() {
            if (node instanceof APNode) {
                return ((APNode) node).getName();
            } else if (node instanceof CANode) {
                return ((CANode) node).getName();
            } else if (node instanceof CPNode) {
                return ((CPNode) node).getName();
            }
            return "Unknown";
        }

        public CAPType getCAPType() {
            if (node instanceof APNode) {
                return ((APNode) node).getCAPType();
            } else if (node instanceof CANode) {
                return ((CANode) node).getCAPType();
            } else if (node instanceof CPNode) {
                return ((CPNode) node).getCAPType();
            }
            return CAPType.AP;
        }

        public Map<String, String> getLocalState() {
            if (node instanceof APNode) {
                return ((APNode) node).getLocalState();
            } else if (node instanceof CANode) {
                return ((CANode) node).getLocalState();
            } else if (node instanceof CPNode) {
                return ((CPNode) node).getLocalState();
            }
            return new HashMap<>();
        }

        public void simulatePartition(boolean partitioned) {
            if (node instanceof APNode) {
                ((APNode) node).simulatePartition(partitioned);
            } else if (node instanceof CANode) {
                ((CANode) node).simulatePartition(partitioned);
            } else if (node instanceof CPNode) {
                ((CPNode) node).simulatePartition(partitioned);
            }
        }
    }

    /**
     * Test worker that performs operations on a CAP node
     */
    private static class TestWorker implements Runnable {
        private final CAPNodeBase node;
        private final TestConfiguration config;
        private final ConsistencyMonitor monitor;
        private final Random random;

        public TestWorker(CAPNodeBase node, TestConfiguration config, ConsistencyMonitor monitor) {
            this.node = node;
            this.config = config;
            this.monitor = monitor;
            this.random = new Random();
        }

        @Override
        public void run() {
            System.out.printf("[%s] 🚀 Test worker started with %s node%n",
                    node.getName(), node.getCAPType());

            // Wait a bit for initialization
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }

            for (int i = 0; i < config.operationsPerNode; i++) {
                try {
                    performOperation(i + 1);

                    // Random delay between operations (additional to built-in node delays)
                    Thread.sleep(200 + random.nextInt(800));

                } catch (Exception e) {
                    System.err.printf("[%s] ❌ Operation %d failed: %s%n",
                            node.getName(), i + 1, e.getMessage());

                    monitor.recordFailedOperation(node.getName(), node.getCAPType(), e.getMessage());

                    // For CA and CP systems, consider stopping on critical failures
                    if (node.getCAPType() != CAPType.AP && e.getMessage().contains("unavailable")) {
                        System.err.printf("[%s] %s system stopping due to availability issues%n",
                                node.getName(), node.getCAPType());
                        break;
                    }
                }
            }

            System.out.printf("[%s] ✅ Test worker finished%n", node.getName());
        }

        private void performOperation(int operationNumber) throws Exception {
            if (random.nextBoolean()) {
                // Write operation
                String key = generateKey();
                String value = node.getName() + "_" + operationNumber;

                long startTime = System.currentTimeMillis();
                try {
                    node.writeValue(key, value);
                    long duration = System.currentTimeMillis() - startTime;
                    monitor.recordWrite(node.getName(), key, value, true, duration);
                } catch (Exception e) {
                    long duration = System.currentTimeMillis() - startTime;
                    monitor.recordWrite(node.getName(), key, value, false, duration);
                    throw e;
                }
            } else {
                // Read operation
                String key = generateKey();

                long startTime = System.currentTimeMillis();
                try {
                    String value = node.readValue(key);
                    long duration = System.currentTimeMillis() - startTime;
                    monitor.recordRead(node.getName(), key, value, true, duration);
                } catch (Exception e) {
                    long duration = System.currentTimeMillis() - startTime;
                    monitor.recordRead(node.getName(), key, null, false, duration);
                    throw e;
                }
            }
        }

        private String generateKey() {
            // Use shared keys to create contention
            String[] sharedKeys = { "counter", "status", "data", "config", "state" };
            return sharedKeys[random.nextInt(sharedKeys.length)];
        }
    }

    /**
     * Simulate network partitions during the test
     */
    private static void simulateNetworkPartitions(Map<String, CAPNodeBase> nodes, int durationSeconds) {
        new Thread(() -> {
            try {
                // Wait for initial operations
                Thread.sleep(3000);

                // Create partition for part of the simulation
                int partitionDuration = durationSeconds * 1000 / 3;

                System.out.println("\n🔥 Simulating network partition...");

                // Partition half the nodes
                int count = 0;
                for (CAPNodeBase node : nodes.values()) {
                    if (count++ < nodes.size() / 2) {
                        node.simulatePartition(true);
                    }
                }

                Thread.sleep(partitionDuration);

                System.out.println("🔧 Resolving network partition...");

                // Resolve partition
                for (CAPNodeBase node : nodes.values()) {
                    node.simulatePartition(false);
                }

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }).start();
    }

    /**
     * Display test results and analysis
     */
    private static void displayResults(Map<String, CAPNodeBase> nodes, ConsistencyMonitor monitor) {
        System.out.println("\n" + "=".repeat(80));
        System.out.println("📊 CAP NODE TEST RESULTS");
        System.out.println("=".repeat(80));

        // Display consistency analysis
        System.out.println("\n🔍 Consistency Analysis:");
        monitor.displayAnalysis();

        // Display final state comparison
        System.out.println("\n🗂️  Final State Comparison:");
        displayStateComparison(nodes);

        // Display CAP theorem analysis
        CAPType capType = nodes.values().iterator().next().getCAPType();
        System.out.println("\n📋 CAP Theorem Analysis:");
        displayCAPAnalysis(capType);
    }

    /**
     * Display state comparison across all nodes
     */
    private static void displayStateComparison(Map<String, CAPNodeBase> nodes) {
        Map<String, Map<String, String>> nodeStates = new HashMap<>();

        for (CAPNodeBase node : nodes.values()) {
            nodeStates.put(node.getName(), node.getLocalState());
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

    /**
     * Display CAP theorem analysis
     */
    private static void displayCAPAnalysis(CAPType capType) {
        switch (capType) {
            case AP:
                System.out.println("  📍 AP Node System (Availability + Partition Tolerance):");
                System.out.println("    ✅ High availability - operations continue during partitions");
                System.out.println("    ✅ Partition tolerant - system remains operational");
                System.out.println("    ⚠️  Eventual consistency - temporary inconsistencies expected");
                System.out.println("    🔧 Architecture: Each node manages its own read/write operations");
                break;

            case CA:
                System.out.println("  📍 CA Node System (Consistency + Availability):");
                System.out.println("    ✅ Strong consistency - all operations maintain consistency");
                System.out.println("    ✅ High availability - fast operations when network is stable");
                System.out.println("    ❌ No partition tolerance - fails during network splits");
                System.out.println("    🔧 Architecture: Coordinator-based with synchronous propagation");
                break;

            case CP:
                System.out.println("  📍 CP Node System (Consistency + Partition Tolerance):");
                System.out.println("    ✅ Strong consistency - quorum ensures data consistency");
                System.out.println("    ✅ Partition tolerant - continues with majority quorum");
                System.out.println("    ⚠️  Limited availability - may block when quorum unavailable");
                System.out.println("    🔧 Architecture: Quorum-based consensus with distributed coordination");
                break;
        }
    }

    /**
     * Configuration for the CAP node test
     */
    public static class TestConfiguration {
        CAPType capType = CAPType.AP;
        int nodeCount = DEFAULT_NODE_COUNT;
        int operationsPerNode = DEFAULT_OPERATIONS_PER_NODE;
        int simulationDurationSeconds = 20;
        boolean simulatePartitions = false;
    }
}
