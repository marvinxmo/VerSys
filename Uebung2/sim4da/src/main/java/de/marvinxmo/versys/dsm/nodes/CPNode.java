package de.marvinxmo.versys.dsm.nodes;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import de.marvinxmo.versys.Message;
import de.marvinxmo.versys.NetworkConnection;
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
public class CPNode extends DSMNode {

    public class Quorum {

        public String id;
        public boolean isActive = true;

        public String keyForEdit;
        public String newValue;
        public String initiator;

        public List<String> approver;
        public int approvalsNeeded;

        public NetworkConnection connection;

        public Quorum(String id, String keyForEdit, String newValue, String initiator) {
            this.id = id;
            this.keyForEdit = keyForEdit;
            this.newValue = newValue;
            this.initiator = initiator;

            // Initialize with empty approvers
            this.approver = new ArrayList<String>();
            this.approver.add(initiator);

            this.approvalsNeeded = CPNode.approvalsNeeded;

            this.connection = new NetworkConnection(id);

            // Start the thread to listen for approval messages
            Thread approvalListenerThread = new Thread(this::receiveApprovals);
            approvalListenerThread.setName("Quorum-" + id + "-Approval-Thread");
            approvalListenerThread.setDaemon(true); // Make it a daemon thread so it doesn't block program exit
            approvalListenerThread.start();

            this.sendApprovalRequests();

            System.out.printf("[%s] Started new quorum %s for [%s=%s] %n",
                    CPNode.this.getName(), id, keyForEdit, newValue);

        }

        @Override
        public String toString() {
            return String.format("Quorum[id=%s, key=%s, value=%s, Approvals=%d, required=%d]",
                    id, keyForEdit, newValue, approver.size(), approvalsNeeded);
        }

        public void sendApprovalRequests() {
            Message message = new Message();
            message.add("type", "QUORUM_APPROVAL_REQUEST");
            message.add("quorumId", this.id);
            message.add("keyForEdit", this.keyForEdit);
            message.add("newValue", this.newValue);

            try {
                broadcast(message);
            } catch (Exception e) {
                System.err.printf("[%s] Error sending approval requests: %s%n", getName(), e.getMessage());
            }
        }

        public void receiveApprovals() {
            while (isActive) {
                try {

                    Message message = connection.receive();

                    if (message == null) {
                        continue; // No message received, continue waiting
                    }

                    String type = message.query("type");

                    if ("QUORUM_APPROVAL".equals(type)) {
                        String approverNode = message.queryHeader("sender");
                        if (!approver.contains(approverNode)) {
                            approver.add(approverNode);
                            System.out.printf("[%s] Received approval from %s for quorum %s [%s = %s]%n",
                                    getName(), approverNode, id, keyForEdit, newValue);
                        }
                        if (approver.size() >= approvalsNeeded) {
                            isActive = false; // Quorum achieved
                            System.out.printf("[%s] Quorum %s [%s = %s] achieved sufficient Approvals %n",
                                    getName(), id, keyForEdit, newValue);
                            writeToDSM();
                        }
                    }
                } catch (Exception e) {
                    System.err.printf("[%s] Error receiving approvals: %s%n", getName(), e.getMessage());
                }
            }
        }

        public void writeToDSM() {
            if (approver.size() >= approvalsNeeded) {
                VersionedValue newValueObj = new VersionedValue(newValue, System.currentTimeMillis(), this.initiator);
                storage.put(this.keyForEdit, newValueObj);
                System.out.printf("[%s] Quorum %s wrote to DSM: %s = %s%n",
                        getName(), id, keyForEdit, newValue);
            } else {
                System.out.printf("[%s] Quorum %s did not achieve enough approvals to write: %s = %s%n",
                        getName(), id, keyForEdit, newValue);
            }
        }
    }

    public static int approvalsNeeded = 3;
    public static Map<String, VersionedValue> storage = new ConcurrentHashMap<String, VersionedValue>();
    public static List<Quorum> quorums = new ArrayList<Quorum>();

    private final AtomicBoolean running;

    // Futures to control individual tasks
    private Future<?> messageProcessingTask;
    private Future<?> writeLoopTask;
    private Future<?> readLoopTask;
    private Future<?> partitionTask;

    public CPNode(String name) {
        super(name);
        this.executorService = Executors.newFixedThreadPool(4); // Increased to 4 for message processing
        this.running = new AtomicBoolean(false);
    }

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

        System.out.printf("[%s] CP Node started with concurrent operations%n", getName());

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

        if ("QUORUM_APPROVAL_REQUEST".equals(messageType)) {
            handleApprovalRequest(message);
            return;
        }

        System.out.printf("[%s] Received unsupported message type: %s%n",
                getName(), messageType);

    }

    private void handleApprovalRequest(Message message) {

        Random r = new Random();
        if (r.nextDouble() < 0.2) {
            // Ignore or decline with 20% chance
            return;
        }

        if (!this.messageProcessingEnabled) {
            // Node is Partitioned -> Cant approve
            return;
        }

        try {
            Message response = new Message();
            response.add("type", "QUORUM_APPROVAL");

            try {
                if (simulateNetworkLatency) {
                    sleep(getLatencyMs());
                }
                send(response, message.query("quorumId"));
                // System.out.println("send approval to" + message.query("quorumId"));
            } catch (Exception e) {
                System.out.printf("[%s] Error sending approval : %s%n",
                        getName(), e.getMessage());
            }

        } catch (Exception e) {
            System.err.printf("[%s] Error handling approval request: %s%n",
                    getName(), e.getMessage());
        }
    }

    public void randomWriteLoop() {

        while (this.isAlive() && !Thread.currentThread().isInterrupted()) {
            // Random pause before operation
            int pauseMillis = new Random().nextInt(2000, 7000);
            sleep(pauseMillis);

            if (Thread.currentThread().isInterrupted() || !this.isAlive()) {
                break;
            }

            String key = this.getRandomKey();
            String rstr = new RandomString(8).nextString();

            // Try to create Quorum (will fail during partition due to disabled message
            // processing)
            if (this.messageProcessingEnabled) {

                try {
                    Quorum quo = new Quorum("Q-" + new RandomString(4).nextString(), key, rstr, getName());
                    CPNode.quorums.add(quo);

                } catch (Exception sendError) {
                    System.err.printf("[%s] Quorum initiation failed: %s%n", getName(), sendError.getMessage());
                }
            } else {
                System.out.printf("[%s] Cant initiate quorum - node is partitioned%n", getName());
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

                boolean partitioned = !this.messageProcessingEnabled;

                if (partitioned) {
                    System.out.printf("[%s] Cant read atm - node is partitioned%n", getName());
                    continue;
                }

                VersionedValue vv = CPNode.storage.get(key);

                if (vv == null) {
                    vv = new VersionedValue("empty", 0, "none");
                }

                System.out.printf(
                        "[%s] Sucessfully READ: %s = %s (written by %s at %d) [Partitioned: %s]%n",
                        getName(), key, vv.value, vv.lastUpdater, vv.timestamp, partitioned);

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

}