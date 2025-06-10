package de.marvinxmo.versys.dsm;

import java.util.HashSet;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;

import de.marvinxmo.versys.Message;
import de.marvinxmo.versys.Simulator;

/**
 * Interactive test application for all three DSM implementations
 * Allows user to choose between AP, CA, and CP DSM configurations
 */
public class DSMTestApplication {

    public enum DSMType {
        AP("Availability + Partition Tolerance", "High availability, eventual consistency"),
        CA("Consistency + Availability", "Strong consistency, no partition tolerance"),
        CP("Consistency + Partition Tolerance", "Strong consistency, may block during partitions");

        private final String fullName;
        private final String description;

        DSMType(String fullName, String description) {
            this.fullName = fullName;
            this.description = description;
        }

        public String getFullName() {
            return fullName;
        }

        public String getDescription() {
            return description;
        }
    }

    /**
     * Test node that uses DSM for distributed counter management
     */
    public static class CounterNode extends DSMNode {
        private final int nodeId;
        private final DSMType dsmType;
        private int localCounter = 0;
        private final int maxOperations;
        private int operationCount = 0;

        public CounterNode(String name, int nodeId, DSMType dsmType, int maxOperations) {
            super(name);
            this.nodeId = nodeId;
            this.dsmType = dsmType;
            this.maxOperations = maxOperations;
        }

        @Override
        protected void engage() {
            System.out.printf("[%s] Node started with %s DSM%n", getName(), dsmType);

            // Wait a bit for all nodes to initialize
            sleep(200);

            while (operationCount < maxOperations) {
                try {
                    performCounterOperations();
                } catch (Exception e) {
                    System.err.printf("[%s] Operation failed: %s%n", getName(), e.getMessage());
                    if (dsmType == DSMType.CA || dsmType == DSMType.CP) {
                        // CA and CP systems should handle failures differently
                        System.err.printf("[%s] %s system encountered critical failure%n", getName(), dsmType);
                        break;
                    }
                }

                // Variable delay based on DSM type
                int baseDelay = 800;
                int variableDelay = (int) (Math.random() * 400); // 0-400ms
                sleep(baseDelay + variableDelay);
            }

            System.out.printf("[%s] Node finished after %d operations%n", getName(), operationCount);
        }

        private void performCounterOperations() {
            operationCount++;

            // Increment local counter and write to DSM
            localCounter++;
            String counterKey = "counter_" + nodeId;

            try {
                dsm.write(counterKey, String.valueOf(localCounter));
                System.out.printf("[%s] Operation %d: Incremented counter to %d%n",
                        getName(), operationCount, localCounter);
            } catch (Exception e) {
                System.err.printf("[%s] Write failed in operation %d: %s%n",
                        getName(), operationCount, e.getMessage());
                throw e; // Re-throw to be handled by engage()
            }

            // Read other nodes' counters to check for inconsistencies
            checkOtherCounters();

            // Occasionally perform additional operations based on DSM type
            if (operationCount % 3 == 0) {
                performSpecialOperation();
            }
        }

        private void checkOtherCounters() {
            System.out.printf("[%s] Checking other nodes' counters:%n", getName());

            for (int i = 0; i < 3; i++) { // Assuming 3 nodes
                if (i != nodeId) {
                    String counterKey = "counter_" + i;
                    try {
                        String value = dsm.read(counterKey);

                        if (value != null) {
                            System.out.printf("[%s]   Node %d counter: %s%n", getName(), i, value);
                        } else {
                            System.out.printf("[%s]   Node %d counter: NOT_FOUND%n", getName(), i);
                            if (dsmType == DSMType.CA) {
                                System.out.printf("[%s]   WARNING: CA system should never have missing data!%n",
                                        getName());
                            }
                        }
                    } catch (Exception e) {
                        System.err.printf("[%s]   Failed to read Node %d counter: %s%n",
                                getName(), i, e.getMessage());
                    }
                }
            }

            // Check consistency of own counter
            try {
                String ownValue = dsm.read("counter_" + nodeId);
                if (ownValue != null && !ownValue.equals(String.valueOf(localCounter))) {
                    System.out.printf("[%s] INCONSISTENCY DETECTED: Local=%d, DSM=%s%n",
                            getName(), localCounter, ownValue);
                }
            } catch (Exception e) {
                System.err.printf("[%s] Failed to read own counter: %s%n", getName(), e.getMessage());
            }
        }

        private void performSpecialOperation() {
            // Perform DSM-type specific operations to highlight characteristics
            switch (dsmType) {
                case AP:
                    // AP: Test high availability - write shared data
                    try {
                        dsm.write("shared_data", "node_" + nodeId + "_" + System.currentTimeMillis());
                        System.out.printf("[%s] AP: Wrote to shared data (should be highly available)%n", getName());
                    } catch (Exception e) {
                        System.err.printf("[%s] AP: Unexpected failure in high-availability write: %s%n", getName(),
                                e.getMessage());
                    }
                    break;

                case CA:
                    // CA: Test strong consistency - read after write
                    try {
                        String testKey = "consistency_test_" + nodeId;
                        String testValue = "consistent_" + operationCount;
                        dsm.write(testKey, testValue);
                        String readValue = dsm.read(testKey);
                        if (!testValue.equals(readValue)) {
                            System.err.printf("[%s] CA: CONSISTENCY VIOLATION! Written=%s, Read=%s%n",
                                    getName(), testValue, readValue);
                        } else {
                            System.out.printf("[%s] CA: Consistency verified (write-then-read)%n", getName());
                        }
                    } catch (Exception e) {
                        System.err.printf("[%s] CA: Failed consistency test: %s%n", getName(), e.getMessage());
                        throw e;
                    }
                    break;

                case CP:
                    // CP: Test partition tolerance with consistency - attempt quorum operation
                    try {
                        String quorumKey = "quorum_test_" + nodeId;
                        String quorumValue = "quorum_" + operationCount;
                        dsm.write(quorumKey, quorumValue);
                        System.out.printf("[%s] CP: Quorum write successful (consistency maintained)%n", getName());
                    } catch (Exception e) {
                        System.err.printf(
                                "[%s] CP: Quorum write failed (availability sacrificed for consistency): %s%n",
                                getName(), e.getMessage());
                        // In CP system, this is acceptable behavior during partitions
                    }
                    break;
            }
        }

        @Override
        protected void handleApplicationMessage(Message message) {
            // This application doesn't use custom messages beyond DSM sync
            System.out.printf("[%s] Received unknown application message: %s%n", getName(), message);
        }
    }

    /**
     * Display menu and get user choice
     */
    @SuppressWarnings("resource") // Scanner wraps System.in, which should not be closed
    private static DSMType getUserChoice() {
        Scanner scanner = new Scanner(System.in);

        System.out.println("\n=== Distributed Shared Memory Test Application ===");
        System.out.println("Choose a DSM implementation to test:");
        System.out.println();

        DSMType[] types = DSMType.values();
        for (int i = 0; i < types.length; i++) {
            System.out.printf("%d. %s (%s)%n", i + 1, types[i].getFullName(), types[i].getDescription());
        }

        System.out.println();
        System.out.print("Enter your choice (1-3): ");

        while (true) {
            try {
                int choice = scanner.nextInt();
                if (choice >= 1 && choice <= types.length) {
                    return types[choice - 1];
                } else {
                    System.out.print("Invalid choice. Please enter 1, 2, or 3: ");
                }
            } catch (Exception e) {
                System.out.print("Invalid input. Please enter a number (1-3): ");
                scanner.nextLine(); // Clear invalid input
            }
        }
    }

    /**
     * Create DSM implementation based on user choice
     */
    private static DSM createDSM(DSMType type, DSMNode node, Set<String> knownNodes, int nodeIndex) {
        switch (type) {
            case AP:
                return new AP_DSM(node, knownNodes);
            case CA:
                // First node acts as central coordinator
                boolean isCoordinator = (nodeIndex == 0);
                return new CA_DSM(node, knownNodes, isCoordinator);
            case CP:
                return new CP_DSM(node, knownNodes);
            default:
                throw new IllegalArgumentException("Unknown DSM type: " + type);
        }
    }

    /**
     * Main method to run the DSM test
     */
    public static void main(String[] args) throws InterruptedException {
        DSMType chosenType = getUserChoice();

        System.out.println("\n=== Starting " + chosenType.getFullName() + " Test ===");
        System.out.println("Description: " + chosenType.getDescription());
        System.out.println();

        Simulator simulator = Simulator.getInstance();

        // Create known nodes set
        Set<String> knownNodes = new HashSet<>();
        knownNodes.add("node0");
        knownNodes.add("node1");
        knownNodes.add("node2");

        // Configuration based on DSM type
        int maxOperations = 8;
        int simulationDuration = 20; // seconds

        if (chosenType == DSMType.CP) {
            // CP may need more time due to quorum operations
            simulationDuration = 25;
            maxOperations = 6; // Fewer operations due to potential blocking
        } else if (chosenType == DSMType.CA) {
            // CA should be fast and consistent
            maxOperations = 10;
            simulationDuration = 15;
        }

        // Create and initialize nodes
        CounterNode[] nodes = new CounterNode[3];
        for (int i = 0; i < 3; i++) {
            nodes[i] = new CounterNode("node" + i, i, chosenType, maxOperations);
            DSM dsmImpl = createDSM(chosenType, nodes[i], knownNodes, i);
            nodes[i].initializeDSM(dsmImpl);

            if (chosenType == DSMType.CA && i == 0) {
                System.out.printf("Node %d configured as central coordinator for CA system%n", i);
            }
        }

        System.out.println("Starting simulation for " + simulationDuration + " seconds...");
        System.out.println("Watch for differences in behavior between DSM types:");

        switch (chosenType) {
            case AP:
                System.out.println("- AP: Look for eventual consistency and high availability");
                System.out.println("- AP: May see temporary inconsistencies that resolve over time");
                break;
            case CA:
                System.out.println("- CA: Look for immediate consistency and coordinator-based operations");
                System.out.println("- CA: Should never see inconsistencies but may fail on network issues");
                break;
            case CP:
                System.out.println("- CP: Look for quorum-based operations and potential blocking");
                System.out.println("- CP: May see some operations fail to maintain consistency");
                break;
        }

        System.out.println();

        // Start simulation
        simulator.simulate(simulationDuration);

        // Print final state analysis
        System.out.println("\n=== Final Analysis ===");
        printFinalAnalysis(nodes, chosenType);

        simulator.shutdown();
        System.out.println("\nSimulation completed. Check the output above for DSM behavior characteristics.");
    }

    /**
     * Print analysis of the final state
     */
    private static void printFinalAnalysis(CounterNode[] nodes, DSMType dsmType) {
        System.out.printf("Final state analysis for %s:%n%n", dsmType.getFullName());

        boolean allConsistent = true;

        for (int i = 0; i < nodes.length; i++) {
            System.out.printf("Node %d final state:%n", i);
            DSM dsm = nodes[i].getDSM();

            Map<String, String> localState;
            if (dsm instanceof AP_DSM) {
                localState = ((AP_DSM) dsm).getLocalState();
            } else if (dsm instanceof CA_DSM) {
                localState = ((CA_DSM) dsm).getLocalState();
            } else if (dsm instanceof CP_DSM) {
                localState = ((CP_DSM) dsm).getLocalState();
            } else {
                localState = new java.util.HashMap<>();
            }

            for (String key : localState.keySet()) {
                String value = localState.get(key);
                System.out.printf("  %s = %s%n", key, value);
            }
            System.out.println();
        }

        // Check consistency across nodes
        System.out.println("Consistency Analysis:");

        // Check counter consistency
        for (int counterId = 0; counterId < nodes.length; counterId++) {
            String counterKey = "counter_" + counterId;
            Set<String> values = new HashSet<>();

            for (CounterNode node : nodes) {
                DSM dsm = node.getDSM();
                Map<String, String> localState;

                if (dsm instanceof AP_DSM) {
                    localState = ((AP_DSM) dsm).getLocalState();
                } else if (dsm instanceof CA_DSM) {
                    localState = ((CA_DSM) dsm).getLocalState();
                } else if (dsm instanceof CP_DSM) {
                    localState = ((CP_DSM) dsm).getLocalState();
                } else {
                    continue;
                }

                String value = localState.get(counterKey);
                if (value != null) {
                    values.add(value);
                }
            }

            if (values.size() > 1) {
                System.out.printf("INCONSISTENCY: %s has different values: %s%n", counterKey, values);
                allConsistent = false;
            } else if (values.size() == 1) {
                System.out.printf("CONSISTENT: %s = %s across all nodes%n", counterKey, values.iterator().next());
            } else {
                System.out.printf("NO DATA: %s not found on any node%n", counterKey);
            }
        }

        System.out.println();

        // Type-specific analysis
        switch (dsmType) {
            case AP:
                if (allConsistent) {
                    System.out.println("✓ AP Result: Eventually consistent state achieved!");
                } else {
                    System.out.println("? AP Result: Some inconsistencies remain (this is acceptable in AP systems)");
                }
                System.out.println("AP systems prioritize availability - operations should rarely fail");
                break;

            case CA:
                if (allConsistent) {
                    System.out.println("✓ CA Result: Strong consistency maintained!");
                } else {
                    System.out.println(
                            "✗ CA Result: Consistency violation detected (this should not happen in CA systems)");
                }
                System.out.println("CA systems should never show inconsistencies but may fail during network issues");
                break;

            case CP:
                if (allConsistent) {
                    System.out.println("✓ CP Result: Strong consistency maintained despite potential partitions!");
                } else {
                    System.out.println(
                            "? CP Result: Some inconsistencies (unusual for CP, may indicate implementation issues)");
                }
                System.out.println(
                        "CP systems maintain consistency even if some operations fail due to insufficient quorum");
                break;
        }
    }
}
