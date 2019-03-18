package it.polimi.distsys.paxos.utils;

import java.io.Serializable;

public class NodeRef implements Serializable {
    private static NodeRef self;
    private String ip;
    private int port;
    private int id;

    public NodeRef(final String ip, final int port) {
        this.ip = ip;
        this.port = port;
        this.id = (port + ip).hashCode();
    }

    public String getIp() {
        return ip;
    }

    public int getPort() {
        return port;
    }

    public int getId() {
        return id;
    }

    public static void setSelf(NodeRef s) {
        self = s;
    }

    public static NodeRef getSelf() {
        return self;
    }
}
