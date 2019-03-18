package it.polimi.distsys.paxos.communication.messages;

import it.polimi.distsys.paxos.utils.NodeRef;

import java.io.Serializable;

public class CommunicationMessage implements Serializable {
    private NodeRef src;
    private NodeRef dst;
    private Serializable body;

    public CommunicationMessage(final NodeRef src, final NodeRef dst, final Serializable body) {
        this.src = src;
        this.dst = dst;
        this.body = body;
    }

    public NodeRef getSrc() {
        return src;
    }

    public NodeRef getDst() {
        return dst;
    }

    public Serializable getBody() {
        return body;
    }
}
