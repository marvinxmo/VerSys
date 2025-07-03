package de.marvinxmo.versys.dsm.old;

import de.marvinxmo.versys.Message;

/**
 * Message for synchronizing DSM state between nodes
 */
public class DSMSyncMessage extends Message {

    public enum Type {
        WRITE_PROPAGATION,
        GOSSIP_SYNC
    }

    public DSMSyncMessage(Type type, String key, String value, long timestamp,
            String originNodeId) {
        super();

        // Store data in the Message payload
        add("type", type.toString());
        add("key", key);
        add("value", value);
        add("timestamp", String.valueOf(timestamp));
        add("originNodeId", originNodeId);
    }

    public Type getType() {
        return Type.valueOf(query("type"));
    }

    public String getKey() {
        return query("key");
    }

    public String getValue() {
        return query("value");
    }

    public long getTimestamp() {
        return Long.parseLong(query("timestamp"));
    }

    public String getOriginNodeId() {
        return query("originNodeId");
    }

    @Override
    public String toString() {
        return String.format("DSMSyncMessage[type=%s, key=%s, value=%s, timestamp=%d, origin=%s]",
                getType(), getKey(), getValue(), getTimestamp(), getOriginNodeId());
    }
}