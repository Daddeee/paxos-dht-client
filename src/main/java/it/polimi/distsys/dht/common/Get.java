package it.polimi.distsys.dht.common;

import it.polimi.distsys.paxos.protocol.ProposalValue;
import it.polimi.distsys.paxos.utils.NodeRef;

import java.io.Serializable;

public class Get extends DHTMessage implements Serializable {
    private String key;

    public Get(String key, long timeout, long timestamp, NodeRef replyNode) {
        super(timeout, timestamp, replyNode);
        this.key = key;
    }

    public String getKey() {
        return key;
    }
}
