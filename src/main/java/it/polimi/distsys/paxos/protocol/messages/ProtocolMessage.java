package it.polimi.distsys.paxos.protocol.messages;

import java.io.Serializable;

public abstract class ProtocolMessage implements Serializable {
    private int from;

    public ProtocolMessage(int from) {
        this.from = from;
    }

    public int getFrom() {
        return from;
    }
}
