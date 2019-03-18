package it.polimi.distsys.paxos.network;

import it.polimi.distsys.paxos.communication.Sender;
import it.polimi.distsys.paxos.communication.messages.CommunicationMessage;
import it.polimi.distsys.paxos.network.messages.NetworkMessage;
import it.polimi.distsys.paxos.protocol.messages.ProtocolMessage;
import it.polimi.distsys.paxos.utils.NodeRef;
import it.polimi.distsys.paxos.utils.QueueProducer;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class Forwarder {
    private Map<Integer, NodeRef> receivers;
    private QueueProducer<CommunicationMessage> sendProducer;

    public Forwarder(Sender sender, NodeRef[] all) {
        this.receivers = new ConcurrentHashMap<>();
        Arrays.stream(all).forEach(n -> receivers.put(n.getId(), n));
        this.sendProducer = sender.getSendProducer();
    }

    public int getNumReceivers() {
        return receivers.size();
    }

    public List<Integer> getReceiversIds() {
        return receivers.keySet().stream().sorted().collect(Collectors.toList());
    }

    public void send(ProtocolMessage m, int toId) {
        NetworkMessage message = ProtocolMessageConverter.convert(m, toId);
        receivers.computeIfPresent(message.getToID(), ((id, receiverRef) -> {
            sendProducer.produce(new CommunicationMessage(NodeRef.getSelf(), receiverRef, message));
            return receiverRef;
        }));
    }

    public void broadcast(ProtocolMessage m) {
        receivers.forEach(((id, receiverRef) -> {
            NetworkMessage message = ProtocolMessageConverter.convert(m, receiverRef.getId());
            CommunicationMessage cmessage = new CommunicationMessage(NodeRef.getSelf(), receiverRef, message);
            sendProducer.produce(cmessage);
        }));
    }
}
