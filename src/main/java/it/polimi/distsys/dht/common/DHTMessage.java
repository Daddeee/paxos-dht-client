package it.polimi.distsys.dht.common;

import it.polimi.distsys.paxos.protocol.ProposalValue;
import it.polimi.distsys.paxos.utils.NodeRef;

import java.io.Serializable;

public abstract class DHTMessage implements Serializable, ProposalValue {
    private long timeout;
    private long timestamp;
    private NodeRef replyNode;

    public DHTMessage(long timeout, long timestamp, NodeRef replyNode) {
        this.timeout = timeout;
        this.timestamp = timestamp;
        this.replyNode = replyNode;
    }

    public long getTimeout() {
        return timeout;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public NodeRef getReplyNode() {
        return replyNode;
    }

    public Reply getReply(String value, boolean status) {
        return new Reply(value, status, timeout, timestamp, replyNode);
    }
}
