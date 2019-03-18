package it.polimi.distsys.paxos.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.function.Consumer;

public class QueueConsumer<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(QueueConsumer.class);
    private final BlockingQueue<T> queue;
    private volatile boolean running;

    public QueueConsumer(BlockingQueue<T> queue) {
        this.queue = queue;
        this.running = true;
    }

    public void consume(Consumer<T> consumer) {
        ThreadUtil.getExecutorService().submit(() -> consumeQueue(consumer));
    }

    private void consumeQueue(Consumer<T> consumer) {
        try {
            while (running) {
                consumer.accept(queue.take());
            }
        } catch (InterruptedException e) {
            LOGGER.error("QueueConsumer interrupted while waiting for an object.", e);
        }
    }

    public void stop() {
        this.running = false;
    }
}
