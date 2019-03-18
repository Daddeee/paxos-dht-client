package it.polimi.distsys.paxos.network.messages;

import java.io.Serializable;

public enum NetworkMessageType implements Serializable {
    PROPOSE, PREPARE, PROMISE, ACCEPT, ACCEPTED, DECIDE, HEARTBEAT, DHT
}
