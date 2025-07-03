package de.marvinxmo.versys.dsm.testing;

import java.util.Random;

import de.marvinxmo.versys.Message;
import de.marvinxmo.versys.dsm.core.DSMNode;
import de.marvinxmo.versys.dsm.monitoring.ConsistencyMonitor;

/**
 * Test node that performs distributed counter operations using DSM
 */
public class TestNode extends DSMNode {

    private final DSMTestSuite.TestConfiguration config;
    private final ConsistencyMonitor monitor;
    private final Random random;
    private int operationCount = 0;
    private int localCounter = 0;

    public TestNode(String name, DSMTestSuite.TestConfiguration config, ConsistencyMonitor monitor) {
        super(name);
        this.config = config;
        this.monitor = monitor;
        this.random = new Random();
    }

    @Override
    protected void engage() {
        System.out.printf("[%s] 🚀 Node started with %s DSM%n", getName(), dsm.getCAPType());

        // Wait a bit for all nodes to initialize
        sleep(2000);

        while (operationCount < config.operationsPerNode) {
            try {
                performOperation();
                // Notify monitor of the operation
                monitor.recordOperation(getName(), dsm.getCAPType());

            } catch (Exception e) {
                System.err.printf("[%s] ❌ Operation failed: %s%n", getName(), e.getMessage());

                // Record failed operation
                monitor.recordFailedOperation(getName(), dsm.getCAPType(), e.getMessage());

                if (config.capType.name().equals("CA") || config.capType.name().equals("CP")) {
                    // CA and CP systems should handle failures differently
                    System.err.printf("[%s] %s system encountered critical failure%n",
                            getName(), config.capType);
                    break;
                }
            }

            // Variable delay between operations
            int baseDelay = 500;
            int variableDelay = random.nextInt(1000); // 0-1000ms
            sleep(baseDelay + variableDelay);
        }

        System.out.printf("[%s] ✅ Node finished after %d operations%n", getName(), operationCount);
    }

    /**
     * Perform a random DSM operation
     */
    private void performOperation() throws Exception {
        operationCount++;

        if (random.nextBoolean()) {
            // Write operation
            performWrite();
        } else {
            // Read operation
            performRead();
        }
    }

    /**
     * Perform a write operation
     */
    private void performWrite() throws Exception {
        localCounter++;
        String key = generateKey();
        String value = String.valueOf(localCounter);

        long startTime = System.currentTimeMillis();

        try {
            dsm.write(key, value);
            long duration = System.currentTimeMillis() - startTime;

            System.out.printf("[%s] 📝 WRITE %d: %s = %s (took %dms)%n",
                    getName(), operationCount, key, value, duration);

            // Record successful operation with monitor
            monitor.recordWrite(getName(), key, value, true, duration);

        } catch (Exception e) {
            long duration = System.currentTimeMillis() - startTime;
            monitor.recordWrite(getName(), key, value, false, duration);
            throw e;
        }
    }

    /**
     * Perform a read operation
     */
    private void performRead() throws Exception {
        String key = generateKey();
        long startTime = System.currentTimeMillis();

        try {
            String value = dsm.read(key);
            long duration = System.currentTimeMillis() - startTime;

            System.out.printf("[%s] 📖 READ %d: %s = %s (took %dms)%n",
                    getName(), operationCount, key, value, duration);

            // Record successful operation with monitor
            monitor.recordRead(getName(), key, value, true, duration);

        } catch (Exception e) {
            long duration = System.currentTimeMillis() - startTime;
            monitor.recordRead(getName(), key, null, false, duration);
            throw e;
        }
    }

    /**
     * Generate a key for operations (shared across nodes for contention)
     */
    private String generateKey() {
        // Use shared keys to create contention
        String[] sharedKeys = { "giraffe", "elephant", "lion", "monkey" };
        return sharedKeys[random.nextInt(sharedKeys.length)];
    }

    @Override
    protected void handleApplicationMessage(Message message) {
        // Handle any application-specific messages
        // For this test, we don't have any special application messages
    }

    /**
     * Get the current operation count
     */
    public int getOperationCount() {
        return operationCount;
    }

    /**
     * Get the local counter value
     */
    public int getLocalCounter() {
        return localCounter;
    }
}
