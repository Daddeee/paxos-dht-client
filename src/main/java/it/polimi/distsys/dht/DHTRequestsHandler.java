package it.polimi.distsys.dht;

import it.polimi.distsys.dht.common.Reply;
import it.polimi.distsys.paxos.utils.QueueConsumer;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DHTRequestsHandler {
    private Map<Long, Reply> responses;
    private QueueConsumer<Reply> requestsConsumer;

    public DHTRequestsHandler(QueueConsumer<Reply> requestsConsumer) {
        this.responses = new ConcurrentHashMap<>();
        this.requestsConsumer = requestsConsumer;
        this.requestsConsumer.consume((this::consume));
    }

    public void putPlaceholder(long timestamp) {
        responses.put(timestamp, new Reply(null, false, 0, 0, null));
    }

    public String waitForGetResponse(long timestamp, long timeout) {
        try{
            Reply res = waitForResponse(timestamp, timeout);
            return res.getValue();
        } catch (Exception e) {
            throw new DHTException(e);
        }
    }

    public boolean waitForPutResponse(long timestamp, long timeout) {
        try{
            Reply res = waitForResponse(timestamp, timeout);
            return res.isOk();
        } catch (Exception e) {
            throw new DHTException(e);
        }
    }

    public boolean waitForRemoveResponse(long timestamp, long timeout) {
        try{
            Reply res = waitForResponse(timestamp, timeout);
            return res.isOk();
        } catch (Exception e) {
            throw new DHTException(e);
        }
    }

    private Reply waitForResponse(long timestamp, long timeout) throws InterruptedException, DHTException {
        Reply res = responses.get(timestamp);
        int POLL_RATE = 100;

        while (res.getTimestamp() == 0 && timeout > 0) {
            Thread.sleep(POLL_RATE);
            timeout = timeout - POLL_RATE;
            res = responses.get(timestamp);
        }

        responses.remove(timestamp);
        if(timeout <= 0) throw new DHTException("Timeouted request");
        return res;
    }

    private void consume(Reply m) {
        if(responses.containsKey(m.getTimestamp())) {
            responses.put(m.getTimestamp(), m);
        }
    }
}
