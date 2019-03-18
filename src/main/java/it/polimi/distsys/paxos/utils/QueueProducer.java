package it.polimi.distsys.paxos.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;

public class QueueProducer<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(QueueProducer.class);
    private final BlockingQueue<T> queue;

    public QueueProducer(BlockingQueue<T> queue) {
        this.queue = queue;
    }

    public void produce(T m) {
        ThreadUtil.getExecutorService().submit(() -> produceObject(m));
    }

    private void produceObject(T m) {
        try {
            queue.put(m);
        } catch (InterruptedException e) {
            LOGGER.error("QueueProducer interrupted while waiting to put an object.", e);
        }
    }
}
