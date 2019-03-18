package it.polimi.distsys.paxos.communication;

import it.polimi.distsys.paxos.communication.messages.CommunicationMessage;
import it.polimi.distsys.paxos.utils.QueueConsumer;
import it.polimi.distsys.paxos.utils.QueueProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class Sender {
    private static final Logger LOGGER = LoggerFactory.getLogger(Sender.class);
    private BlockingQueue<CommunicationMessage> sendQueue;
    private QueueConsumer<CommunicationMessage> sender;

    public Sender() {
        this.sendQueue = new LinkedBlockingQueue<>();
        this.sender = new QueueConsumer<>(sendQueue);
        this.sender.consume(this::send);
    }

    public QueueProducer<CommunicationMessage> getSendProducer() {
        return new QueueProducer<>(sendQueue);
    }

    private void send(CommunicationMessage m) {
        try {
            Socket socket = new Socket(m.getDst().getIp(), m.getDst().getPort());
            ObjectOutputStream o = new ObjectOutputStream(socket.getOutputStream());
            o.writeObject(m);
            socket.close();
            o.close();
        } catch (IOException e) {
            //LOGGER.error("Error while sending a messages.", e);
        }
    }
}
