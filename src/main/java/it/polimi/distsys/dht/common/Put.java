package it.polimi.distsys.dht.common;

import it.polimi.distsys.paxos.protocol.ProposalValue;
import it.polimi.distsys.paxos.utils.NodeRef;

import java.io.Serializable;

public class Put extends DHTMessage implements Serializable {
    private String key;
    private String value;

    public Put(String key, String value, long timeout, long timestamp, NodeRef replyNode) {
        super(timeout, timestamp, replyNode);
        this.key = key;
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }
}
