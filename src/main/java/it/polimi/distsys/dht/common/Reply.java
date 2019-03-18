package it.polimi.distsys.dht.common;

import it.polimi.distsys.paxos.utils.NodeRef;

import java.io.Serializable;

public class Reply extends DHTMessage implements Serializable {
    private String value;
    private boolean ok;

    public Reply(String value, boolean ok, long timeout, long timestamp, NodeRef replyNode) {
        super(timeout, timestamp, replyNode);
        this.value = value;
        this.ok = ok;
    }

    public String getValue() {
        return value;
    }

    public boolean isOk() {
        return ok;
    }
}
