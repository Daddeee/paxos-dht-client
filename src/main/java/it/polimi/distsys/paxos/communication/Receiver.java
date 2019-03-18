package it.polimi.distsys.paxos.communication;

import it.polimi.distsys.paxos.communication.messages.CommunicationMessage;
import it.polimi.distsys.paxos.utils.NodeRef;
import it.polimi.distsys.paxos.utils.QueueConsumer;
import it.polimi.distsys.paxos.utils.QueueProducer;
import it.polimi.distsys.paxos.utils.ThreadUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class Receiver {
    private static final Logger LOGGER = LoggerFactory.getLogger(Receiver.class);

    private ServerSocket serverSocket;
    private volatile boolean running;
    private BlockingQueue<CommunicationMessage> recvQueue;
    private QueueProducer<CommunicationMessage> recvProducer;

    public Receiver() throws IOException {
        this.serverSocket = new ServerSocket(NodeRef.getSelf().getPort(), 0, InetAddress.getByName(NodeRef.getSelf().getIp()));
        this.running = true;
        this.recvQueue = new LinkedBlockingQueue<>();
        this.recvProducer = new QueueProducer<>(this.recvQueue);
        ThreadUtil.getExecutorService().submit(() -> {
            while (running) { accept(); }
            close();
        });
    }

    public QueueConsumer<CommunicationMessage> getRecvConsumer() {
        return new QueueConsumer<>(this.recvQueue);
    }

    public void stop() {
        this.running = false;
    }

    private void accept() {
        try {
            Socket socket = serverSocket.accept();
            CommunicationMessage m = (CommunicationMessage) new ObjectInputStream(socket.getInputStream()).readObject();
            recvProducer.produce(m);
        } catch (IOException | ClassNotFoundException e) {
            LOGGER.error("Error while accepting a new object.", e);
        }
    }

    private void close() {
        try {
            serverSocket.close();
        } catch (IOException e) {
            LOGGER.error("Error while closing socket server.", e);
        }
    }
}
