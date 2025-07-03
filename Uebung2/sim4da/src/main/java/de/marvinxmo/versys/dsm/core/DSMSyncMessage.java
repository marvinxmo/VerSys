package de.marvinxmo.versys.dsm.core;

import de.marvinxmo.versys.Message;

/**
 * Message used for synchronization between DSM nodes
 */
public class DSMSyncMessage extends Message {

    public enum Type {
        WRITE_PROPAGATION("Write operation propagation"),
        QUORUM_REQUEST("Quorum-based write request"),
        QUORUM_RESPONSE("Quorum response"),
        COORDINATOR_SYNC("Central coordinator synchronization"),
        HEARTBEAT("Node heartbeat");
        // PARTITION_DETECTION("Network partition detection");

        private final String description;

        Type(String description) {
            this.description = description;
        }

        public String getDescription() {
            return description;
        }
    }

    private final Type type;
    private final String key;
    private final int value;
    private final long timestamp;
    private final String originNodeId;
    private final String messageId;
    private final boolean requiresResponse;

    public DSMSyncMessage(Type type, String key, int value, long timestamp, String originNodeId) {
        this(type, key, value, timestamp, originNodeId, generateMessageId(), false);
    }

    public DSMSyncMessage(Type type, String key,
            int value, long timestamp, String originNodeId,
            String messageId, boolean requiresResponse) {
        this.type = type;
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
        this.originNodeId = originNodeId;
        this.messageId = messageId;
        this.requiresResponse = requiresResponse;
    }

    // Getters
    public Type getType() {
        return type;
    }

    public String getKey() {
        return key;
    }

    public Integer getValue() {
        return value;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getOriginNodeId() {
        return originNodeId;
    }

    public String getMessageId() {
        return messageId;
    }

    public boolean requiresResponse() {
        return requiresResponse;
    }

    public static String generateMessageId() {
        return String.valueOf(System.nanoTime());
    }

    @Override
    public String toString() {
        return String.format("DSMSyncMessage[type=%s, key=%s, value=%s, timestamp=%d, origin=%s, msgId=%s]",
                type, key, value, timestamp, originNodeId, messageId);
    }
}
