package it.polimi.distsys.dht;

public class DHTException extends RuntimeException {
    public DHTException() {
        super();
    }

    public DHTException(String s) {
        super(s);
    }

    public DHTException(Exception e) {
        super(e);
    }
}
