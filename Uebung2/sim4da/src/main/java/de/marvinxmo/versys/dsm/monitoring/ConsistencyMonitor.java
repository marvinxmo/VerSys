package de.marvinxmo.versys.dsm.monitoring;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import de.marvinxmo.versys.dsm.core.CAPType;

/**
 * Monitors consistency across DSM nodes and provides analysis
 */
public class ConsistencyMonitor {

    private final Set<String> nodeNames;
    private final CAPType systemType;
    private final Map<String, NodeOperationStats> nodeStats;
    private final ScheduledExecutorService scheduler;
    private final AtomicLong totalOperations;
    private final AtomicLong totalFailures;
    private final Map<String, String> lastKnownValues; // key -> last value seen
    private volatile boolean monitoring = false;

    /**
     * Statistics for operations on a specific node
     */
    public static class NodeOperationStats {
        public final AtomicLong reads = new AtomicLong(0);
        public final AtomicLong writes = new AtomicLong(0);
        public final AtomicLong failures = new AtomicLong(0);
        public final AtomicLong totalResponseTime = new AtomicLong(0);
        public final AtomicLong operationCount = new AtomicLong(0);
        public volatile long lastOperationTime = System.currentTimeMillis();

        public double getAverageResponseTime() {
            long ops = operationCount.get();
            return ops > 0 ? (double) totalResponseTime.get() / ops : 0.0;
        }

        public double getSuccessRate() {
            long total = operationCount.get();
            return total > 0 ? (double) (total - failures.get()) / total : 1.0;
        }
    }

    public ConsistencyMonitor(Set<String> nodeNames, CAPType systemType) {
        this.nodeNames = nodeNames;
        this.systemType = systemType;
        this.nodeStats = new ConcurrentHashMap<>();
        this.scheduler = Executors.newScheduledThreadPool(1);
        this.totalOperations = new AtomicLong(0);
        this.totalFailures = new AtomicLong(0);
        this.lastKnownValues = new ConcurrentHashMap<>();

        // Initialize stats for each node
        for (String nodeName : nodeNames) {
            nodeStats.put(nodeName, new NodeOperationStats());
        }
    }

    /**
     * Start monitoring (periodic consistency checks)
     */
    public void startMonitoring() {
        monitoring = true;

        // Schedule periodic consistency reports
        scheduler.scheduleAtFixedRate(this::generatePeriodicReport, 10, 20, TimeUnit.SECONDS);
    }

    /**
     * Stop monitoring
     */
    public void stopMonitoring() {
        monitoring = false;
        scheduler.shutdown();
    }

    /**
     * Record a general operation
     */
    public void recordOperation(String nodeName, CAPType systemType) {
        NodeOperationStats stats = nodeStats.get(nodeName);
        if (stats != null) {
            stats.operationCount.incrementAndGet();
            stats.lastOperationTime = System.currentTimeMillis();
            totalOperations.incrementAndGet();
        }
    }

    /**
     * Record a write operation
     */
    public void recordWrite(String nodeName, String key, String value, boolean successful, long responseTime) {
        NodeOperationStats stats = nodeStats.get(nodeName);
        if (stats != null) {
            stats.writes.incrementAndGet();
            stats.operationCount.incrementAndGet();
            stats.totalResponseTime.addAndGet(responseTime);
            stats.lastOperationTime = System.currentTimeMillis();

            if (!successful) {
                stats.failures.incrementAndGet();
                totalFailures.incrementAndGet();
            } else {
                // Update last known value for consistency tracking
                lastKnownValues.put(key, value);
            }

            totalOperations.incrementAndGet();
        }
    }

    /**
     * Record a read operation
     */
    public void recordRead(String nodeName, String key, String value, boolean successful, long responseTime) {
        NodeOperationStats stats = nodeStats.get(nodeName);
        if (stats != null) {
            stats.reads.incrementAndGet();
            stats.operationCount.incrementAndGet();
            stats.totalResponseTime.addAndGet(responseTime);
            stats.lastOperationTime = System.currentTimeMillis();

            if (!successful) {
                stats.failures.incrementAndGet();
                totalFailures.incrementAndGet();
            } else {
                // Check for consistency
                checkConsistency(nodeName, key, value);
            }

            totalOperations.incrementAndGet();
        }
    }

    /**
     * Record a failed operation
     */
    public void recordFailedOperation(String nodeName, CAPType systemType, String reason) {
        NodeOperationStats stats = nodeStats.get(nodeName);
        if (stats != null) {
            stats.failures.incrementAndGet();
            stats.operationCount.incrementAndGet();
            stats.lastOperationTime = System.currentTimeMillis();
            totalFailures.incrementAndGet();
            totalOperations.incrementAndGet();
        }

        System.err.printf("🔍 [%s] %s SYSTEM FAILURE: %s%n", nodeName, systemType, reason);
    }

    /**
     * Check consistency of read values
     */
    private void checkConsistency(String nodeName, String key, String value) {
        String lastKnown = lastKnownValues.get(key);

        // For AP systems, inconsistency is expected and not an error
        if (systemType == CAPType.AP && lastKnown != null && !lastKnown.equals(value)) {
            System.out.printf("🔍 [%s] AP CONSISTENCY NOTE: %s changed from %s to %s (expected in AP systems)%n",
                    nodeName, key, lastKnown, value);
        }

        // For CA and CP systems, inconsistency is a problem
        if ((systemType == CAPType.CA || systemType == CAPType.CP) &&
                lastKnown != null && !lastKnown.equals(value)) {
            System.err.printf("🚨 [%s] %s CONSISTENCY VIOLATION: %s expected %s but got %s%n",
                    nodeName, systemType, key, lastKnown, value);
        }
    }

    /**
     * Generate periodic monitoring report
     */
    private void generatePeriodicReport() {
        if (!monitoring)
            return;

        System.out.println("\n📊 === PERIODIC MONITORING REPORT ===");
        System.out.printf("System Type: %s | Total Operations: %d | Failures: %d%n",
                systemType, totalOperations.get(), totalFailures.get());

        for (Map.Entry<String, NodeOperationStats> entry : nodeStats.entrySet()) {
            NodeOperationStats stats = entry.getValue();
            System.out.printf("  %s: R:%d W:%d Fail:%d SuccessRate:%.2f%% AvgTime:%.1fms%n",
                    entry.getKey(),
                    stats.reads.get(),
                    stats.writes.get(),
                    stats.failures.get(),
                    stats.getSuccessRate() * 100,
                    stats.getAverageResponseTime());
        }
        System.out.println("=====================================\n");
    }

    /**
     * Display comprehensive analysis
     */
    public void displayAnalysis() {
        System.out.printf("📈 Total Operations: %d | Total Failures: %d | Overall Success Rate: %.2f%%%n",
                totalOperations.get(), totalFailures.get(),
                getOverallSuccessRate() * 100);

        System.out.printf("⏱️  System Performance:%n");
        for (Map.Entry<String, NodeOperationStats> entry : nodeStats.entrySet()) {
            NodeOperationStats stats = entry.getValue();
            System.out.printf("    %s: %.1fms avg response time, %.2f%% success rate%n",
                    entry.getKey(), stats.getAverageResponseTime(), stats.getSuccessRate() * 100);
        }

        // CAP-specific analysis
        switch (systemType) {
            case AP:
                System.out.println("🔍 AP System Analysis:");
                System.out.println("    - Eventual consistency achieved through gossip protocol");
                System.out.println("    - Operations continue during network partitions");
                System.out.println("    - Temporary inconsistencies are normal and expected");
                break;

            case CA:
                System.out.println("🔍 CA System Analysis:");
                System.out.println("    - Strong consistency maintained through central coordination");
                System.out.println("    - Fast operations when all nodes are connected");
                if (totalFailures.get() > 0) {
                    System.out.println("    ⚠️  Failures indicate network partition or coordinator issues");
                }
                break;

            case CP:
                System.out.println("🔍 CP System Analysis:");
                System.out.println("    - Strong consistency through quorum-based operations");
                System.out.println("    - System remains available as long as majority quorum exists");
                if (totalFailures.get() > 0) {
                    System.out.println("    ⚠️  Failures indicate insufficient quorum or network issues");
                }
                break;
        }
    }

    /**
     * Get overall success rate
     */
    public double getOverallSuccessRate() {
        long total = totalOperations.get();
        return total > 0 ? (double) (total - totalFailures.get()) / total : 1.0;
    }

    /**
     * Get statistics for a specific node
     */
    public NodeOperationStats getNodeStats(String nodeName) {
        return nodeStats.get(nodeName);
    }
}
