package de.marvinxmo.versys.dsm;

import de.marvinxmo.versys.Message;
import de.marvinxmo.versys.Node;
import de.marvinxmo.versys.UnknownNodeException;

/**
 * Base class for nodes that use Distributed Shared Memory
 */
public abstract class DSMNode extends Node {
    protected DSM dsm;

    public DSMNode(String name) {
        super(name);
    }

    /**
     * Initialize the DSM implementation for this node
     */
    public void initializeDSM(DSM dsmImplementation) {
        this.dsm = dsmImplementation;
    }

    @Override
    protected void engage() {
        // Continuously process incoming messages
        while (true) {
            Message message = receive();
            if (message == null) {
                sleep(10); // Small delay to prevent busy waiting (save resources)
                continue;
            }

            if (isDSMSyncMessage(message)) {
                // Handle DSM synchronization messages
                if (dsm != null) {
                    DSMSyncMessage dsmMessage = new DSMSyncMessage(
                            DSMSyncMessage.Type.valueOf(message.query("type")),
                            message.query("key"),
                            message.query("value"),
                            Long.parseLong(message.query("timestamp")),
                            message.query("originNodeId"));
                    dsm.handleSyncMessage(dsmMessage);
                }
            } else {
                // Handle other application-specific messages
                handleApplicationMessage(message);
            }
        }
    }

    /**
     * Check if a message is a DSM synchronization message
     */
    private boolean isDSMSyncMessage(Message message) {
        return message.query("type") != null &&
                message.query("key") != null &&
                message.query("timestamp") != null &&
                message.query("originNodeId") != null;
    }

    /**
     * Override this method to handle application-specific messages
     */
    protected abstract void handleApplicationMessage(Message message);

    /**
     * Get the DSM instance for this node
     */
    public DSM getDSM() {
        return dsm;
    }

    /**
     * Get the node name
     */
    public String getName() {
        return NodeName();
    }

    /**
     * Send a message to a specific node (exposed from protected method)
     */
    public void sendMessage(Message message, String toNodeName) throws UnknownNodeException {
        send(message, toNodeName);
    }

    /**
     * Send a message blindly to a specific node (exposed from protected method)
     */
    public void sendMessageBlindly(Message message, String toNodeName) {
        sendBlindly(message, toNodeName);
    }

    /**
     * Broadcast a message to all nodes (exposed from protected method)
     */
    public void broadcastMessage(Message message) {
        broadcast(message);
    }
}
