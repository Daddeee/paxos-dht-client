package it.polimi.distsys.paxos.network.messages;

import java.io.Serializable;

public class NetworkMessage implements Serializable {
    private int fromID;
    private int toID;
    private NetworkMessageType type;
    private Serializable body;

    public NetworkMessage(final int fromID, final int toID, final NetworkMessageType type, final Serializable body) {
        this.fromID = fromID;
        this.toID = toID;
        this.type = type;
        this.body = body;
    }

    public int getFromID() {
        return fromID;
    }

    public int getToID() {
        return toID;
    }

    public NetworkMessageType getType() {
        return type;
    }

    public Serializable getBody() {
        return body;
    }
}
