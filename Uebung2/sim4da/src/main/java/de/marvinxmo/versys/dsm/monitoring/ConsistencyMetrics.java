package de.marvinxmo.versys.dsm.monitoring;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Metrics for tracking consistency and performance of DSM operations
 */
public class ConsistencyMetrics {
    private final AtomicLong totalReads = new AtomicLong(0);
    private final AtomicLong totalWrites = new AtomicLong(0);
    private final AtomicLong successfulReads = new AtomicLong(0);
    private final AtomicLong successfulWrites = new AtomicLong(0);
    private final AtomicLong failedReads = new AtomicLong(0);
    private final AtomicLong failedWrites = new AtomicLong(0);
    private final AtomicLong inconsistentReads = new AtomicLong(0);
    private final AtomicLong conflictResolutions = new AtomicLong(0);
    private final AtomicLong averageResponseTime = new AtomicLong(0);

    // Consistency-specific metrics
    private volatile boolean isConsistent = true;
    private volatile long lastInconsistencyTime = 0;
    private volatile String lastInconsistencyReason = "";

    public void recordRead(boolean successful, boolean consistent, long responseTime) {
        totalReads.incrementAndGet();
        if (successful) {
            successfulReads.incrementAndGet();
        } else {
            failedReads.incrementAndGet();
        }
        if (!consistent) {
            inconsistentReads.incrementAndGet();
            recordInconsistency("Inconsistent read detected");
        }
        updateAverageResponseTime(responseTime);
    }

    public void recordWrite(boolean successful, long responseTime) {
        totalWrites.incrementAndGet();
        if (successful) {
            successfulWrites.incrementAndGet();
        } else {
            failedWrites.incrementAndGet();
        }
        updateAverageResponseTime(responseTime);
    }

    public void recordConflictResolution() {
        conflictResolutions.incrementAndGet();
    }

    public void recordInconsistency(String reason) {
        isConsistent = false;
        lastInconsistencyTime = System.currentTimeMillis();
        lastInconsistencyReason = reason;
    }

    public void markAsConsistent() {
        isConsistent = true;
        lastInconsistencyReason = "";
    }

    private void updateAverageResponseTime(long responseTime) {
        // Simple moving average (simplified implementation)
        long current = averageResponseTime.get();
        long newAverage = (current + responseTime) / 2;
        averageResponseTime.set(newAverage);
    }

    // Getters
    public long getTotalReads() {
        return totalReads.get();
    }

    public long getTotalWrites() {
        return totalWrites.get();
    }

    public long getSuccessfulReads() {
        return successfulReads.get();
    }

    public long getSuccessfulWrites() {
        return successfulWrites.get();
    }

    public long getFailedReads() {
        return failedReads.get();
    }

    public long getFailedWrites() {
        return failedWrites.get();
    }

    public long getInconsistentReads() {
        return inconsistentReads.get();
    }

    public long getConflictResolutions() {
        return conflictResolutions.get();
    }

    public long getAverageResponseTime() {
        return averageResponseTime.get();
    }

    public boolean isConsistent() {
        return isConsistent;
    }

    public long getLastInconsistencyTime() {
        return lastInconsistencyTime;
    }

    public String getLastInconsistencyReason() {
        return lastInconsistencyReason;
    }

    public double getReadSuccessRate() {
        long total = totalReads.get();
        return total > 0 ? (double) successfulReads.get() / total : 0.0;
    }

    public double getWriteSuccessRate() {
        long total = totalWrites.get();
        return total > 0 ? (double) successfulWrites.get() / total : 0.0;
    }

    public double getConsistencyRate() {
        long total = totalReads.get();
        return total > 0 ? (double) (total - inconsistentReads.get()) / total : 1.0;
    }

    @Override
    public String toString() {
        return String.format(
                "ConsistencyMetrics[reads=%d/%d (%.2f%%), writes=%d/%d (%.2f%%), " +
                        "consistency=%.2f%%, conflicts=%d, avgResponseTime=%dms, consistent=%s]",
                successfulReads.get(), totalReads.get(), getReadSuccessRate() * 100,
                successfulWrites.get(), totalWrites.get(), getWriteSuccessRate() * 100,
                getConsistencyRate() * 100, conflictResolutions.get(),
                averageResponseTime.get(), isConsistent);
    }
}
