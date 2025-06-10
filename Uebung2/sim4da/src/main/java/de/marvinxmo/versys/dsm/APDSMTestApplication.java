package de.marvinxmo.versys.dsm;

import java.util.HashSet;
import java.util.Set;

import de.marvinxmo.versys.Message;
import de.marvinxmo.versys.Simulator;

/**
 * Test application to demonstrate AP DSM functionality
 * Each node maintains a counter and periodically reads other nodes' counters
 * to detect inconsistencies
 */
public class APDSMTestApplication {

    /**
     * Test node that uses AP DSM for distributed counter management
     */
    public static class CounterNode extends DSMNode {
        private final int nodeId;
        private int localCounter = 0;
        private final int maxOperations = 10;
        private int operationCount = 0;

        public CounterNode(String name, int nodeId) {
            super(name);
            this.nodeId = nodeId;
        }

        @Override
        protected void engage() {
            System.out.printf("[%s] Node started%n", getName());

            // Wait a bit for all nodes to initialize
            sleep(100);

            while (operationCount < maxOperations) {
                performCounterOperations();
                sleep(500 + (int) (Math.random() * 1000)); // Random delay 0.5-1.5s
            }

            System.out.printf("[%s] Node finished after %d operations%n", getName(), operationCount);
        }

        private void performCounterOperations() {
            operationCount++;

            // Increment local counter and write to DSM
            localCounter++;
            String counterKey = "counter_" + nodeId;
            dsm.write(counterKey, String.valueOf(localCounter));

            System.out.printf("[%s] Operation %d: Incremented counter to %d%n",
                    getName(), operationCount, localCounter);

            // Read other nodes' counters to check for inconsistencies
            checkOtherCounters();
        }

        private void checkOtherCounters() {
            System.out.printf("[%s] Checking other nodes' counters:%n", getName());

            for (int i = 0; i < 3; i++) { // Assuming 3 nodes
                if (i != nodeId) {
                    String counterKey = "counter_" + i;
                    String value = dsm.read(counterKey);

                    if (value != null) {
                        System.out.printf("[%s]   Node %d counter: %s%n", getName(), i, value);
                    } else {
                        System.out.printf("[%s]   Node %d counter: NOT_FOUND (inconsistency detected!)%n",
                                getName(), i);
                    }
                }
            }

            // Also check own counter from DSM perspective
            String ownValue = dsm.read("counter_" + nodeId);
            if (ownValue != null && !ownValue.equals(String.valueOf(localCounter))) {
                System.out.printf("[%s] INCONSISTENCY: Local counter=%d, DSM counter=%s%n",
                        getName(), localCounter, ownValue);
            }
        }

        @Override
        protected void handleApplicationMessage(Message message) {
            // This application doesn't use custom messages beyond DSM sync
            System.out.printf("[%s] Received unknown application message: %s%n", getName(), message);
        }
    }

    /**
     * Main method to run the AP DSM test
     */
    public static void main(String[] args) throws InterruptedException {
        System.out.println("=== AP DSM Test Application ===");
        System.out.println("Testing Availability + Partition Tolerance DSM");
        System.out.println("Each node maintains a counter and checks for inconsistencies");
        System.out.println();

        Simulator simulator = Simulator.getInstance();

        // Create known nodes set
        Set<String> knownNodes = new HashSet<>();
        knownNodes.add("node0");
        knownNodes.add("node1");
        knownNodes.add("node2");

        // Create and initialize nodes
        CounterNode[] nodes = new CounterNode[3];
        for (int i = 0; i < 3; i++) {
            nodes[i] = new CounterNode("node" + i, i);
            AP_DSM dsmImpl = new AP_DSM(nodes[i], knownNodes);
            nodes[i].initializeDSM(dsmImpl);
        }
        // Start simulation for 15 seconds
        System.out.println("Starting simulation...");
        simulator.simulate(15);

        // Print final state
        System.out.println("\n=== Final DSM State ===");
        for (int i = 0; i < 3; i++) {
            System.out.printf("Node %d final state:%n", i);
            AP_DSM dsm = (AP_DSM) nodes[i].getDSM();
            for (String key : dsm.getLocalState().keySet()) {
                String value = dsm.getLocalState().get(key);
                System.out.printf("  %s = %s%n", key, value);
            }
            System.out.println();
        }

        simulator.shutdown();
        System.out.println("Simulation completed.");
    }
}
