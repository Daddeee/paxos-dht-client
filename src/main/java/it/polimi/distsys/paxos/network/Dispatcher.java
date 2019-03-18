package it.polimi.distsys.paxos.network;

import it.polimi.distsys.dht.common.DHTMessage;
import it.polimi.distsys.dht.common.Reply;
import it.polimi.distsys.paxos.communication.Receiver;
import it.polimi.distsys.paxos.communication.messages.CommunicationMessage;
import it.polimi.distsys.paxos.network.messages.NetworkMessage;
import it.polimi.distsys.paxos.protocol.messages.*;
import it.polimi.distsys.paxos.utils.QueueConsumer;
import it.polimi.distsys.paxos.utils.QueueProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class Dispatcher {
    private static final Logger LOGGER = LoggerFactory.getLogger(Dispatcher.class);
    private QueueConsumer<CommunicationMessage> recvConsumer;
    private BlockingQueue<Reply> clientQueue;
    private QueueProducer<Reply> clientQueueProducer;

    public Dispatcher(Receiver receiver) {
        this.recvConsumer = receiver.getRecvConsumer();
        this.recvConsumer.consume(this::dispatch);

        this.clientQueue = new LinkedBlockingQueue<>();
        this.clientQueueProducer = new QueueProducer<>(this.clientQueue);
    }

    public QueueConsumer<Reply> getClientConsumer() {
        return new QueueConsumer<>(this.clientQueue);
    }

    private void dispatch(CommunicationMessage m) {
        NetworkMessage message = (NetworkMessage) m.getBody();

        switch (message.getType()) {
            case DHT:
            clientQueueProducer.produce((Reply) message.getBody());
            break;
            default:
            LOGGER.error("Unrecognized message");
        }
    }
}
