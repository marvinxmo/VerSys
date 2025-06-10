package de.marvinxmo.versys.dsm;

/**
 * Interface for Distributed Shared Memory implementations
 */
public interface DSM {
    /**
     * Write a value to the distributed shared memory
     * 
     * @param key   The key to write to
     * @param value The value to write
     */
    void write(String key, String value);

    /**
     * Read a value from the distributed shared memory
     * 
     * @param key The key to read from
     * @return The value associated with the key, or null if not found
     */
    String read(String key);

    /**
     * Handle incoming synchronization messages from other nodes
     * 
     * @param message The synchronization message
     */
    void handleSyncMessage(DSMSyncMessage message);

    /**
     * Get the current logical timestamp for this node
     * 
     * @return Current logical timestamp
     */
    long getLogicalTimestamp();
}