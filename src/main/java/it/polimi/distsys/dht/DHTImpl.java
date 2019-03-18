package it.polimi.distsys.dht;

import com.google.gson.Gson;
import it.polimi.distsys.dht.common.Get;
import it.polimi.distsys.dht.common.Put;
import it.polimi.distsys.dht.common.Remove;
import it.polimi.distsys.paxos.communication.Receiver;
import it.polimi.distsys.paxos.communication.Sender;
import it.polimi.distsys.paxos.network.Dispatcher;
import it.polimi.distsys.paxos.network.Forwarder;
import it.polimi.distsys.paxos.protocol.messages.Propose;
import it.polimi.distsys.paxos.utils.NodeRef;
import it.polimi.distsys.paxos.utils.ThreadUtil;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

public class DHTImpl implements DHT {
    //Network level
    private Dispatcher dispatcher;
    private Forwarder forwarder;

    //Communication level
    private Receiver receiver;
    private Sender sender;

    //DHT level
    private DHTRequestsHandler dhtRequestsHandler;

    private Random rand;

    public DHTImpl(String myIp, int myPort, String replicasFile) throws IOException {
        NodeRef.setSelf(new NodeRef(myIp, myPort));
        NodeRef[] replicas = parseNodeRefs(replicasFile);
        this.receiver = new Receiver();
        this.sender = new Sender();
        this.dispatcher = new Dispatcher(this.receiver);
        this.forwarder = new Forwarder(this.sender, replicas);
        this.rand = new Random();
        this.dhtRequestsHandler = new DHTRequestsHandler(this.dispatcher.getClientConsumer());
    }

    @Override
    public CompletableFuture<String> get(final String key, final long timeout) {
        Get p = new Get(key, timeout, System.currentTimeMillis() + rand.nextInt(), NodeRef.getSelf());
        int replicaId = this.forwarder.getReceiversIds().get(rand.nextInt(this.forwarder.getReceiversIds().size()));
        this.dhtRequestsHandler.putPlaceholder(p.getTimestamp());
        this.forwarder.send(new Propose(p), replicaId);
        return CompletableFuture.supplyAsync(() -> dhtRequestsHandler.waitForGetResponse(p.getTimestamp(), timeout), ThreadUtil.getExecutorService());
    }

    @Override
    public CompletableFuture<Boolean> put(final String key, final String value, final long timeout) {
        Put p = new Put(key, value, timeout, System.currentTimeMillis() + rand.nextInt(), NodeRef.getSelf());
        int replicaId = this.forwarder.getReceiversIds().get(rand.nextInt(this.forwarder.getReceiversIds().size()));
        this.dhtRequestsHandler.putPlaceholder(p.getTimestamp());
        this.forwarder.send(new Propose(p), replicaId);
        return CompletableFuture.supplyAsync(() -> dhtRequestsHandler.waitForPutResponse(p.getTimestamp(), timeout), ThreadUtil.getExecutorService());
    }

    @Override
    public CompletableFuture<Boolean> remove(final String key, final long timeout) {
        Remove p = new Remove(key, timeout, System.currentTimeMillis() + rand.nextInt(), NodeRef.getSelf());
        int replicaId = this.forwarder.getReceiversIds().get(rand.nextInt(this.forwarder.getReceiversIds().size()));
        this.dhtRequestsHandler.putPlaceholder(p.getTimestamp());
        this.forwarder.send(new Propose(p), replicaId);
        return CompletableFuture.supplyAsync(() -> dhtRequestsHandler.waitForRemoveResponse(p.getTimestamp(), timeout), ThreadUtil.getExecutorService());
    }

    private NodeRef[] parseNodeRefs(String filename) throws IOException {
        Gson gson = new Gson();
        String properties = readFile(filename, Charset.defaultCharset());
        TmpNodeRef[] tmp = gson.fromJson(properties, TmpNodeRef[].class);
        NodeRef[] receivers = new NodeRef[tmp.length];
        for(int i = 0; i < tmp.length; i++)
            receivers[i] = new NodeRef(tmp[i].ip, tmp[i].port);
        return receivers;
    }

    private String readFile(String path, Charset encoding) throws IOException {
        byte[] encoded = Files.readAllBytes(Paths.get(path));
        return new String(encoded, encoding);
    }

    private static class TmpNodeRef {
        public String ip;
        public int port;
    }
}
