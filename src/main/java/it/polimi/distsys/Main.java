package it.polimi.distsys;

import it.polimi.distsys.dht.DHT;
import it.polimi.distsys.dht.DHTImpl;
import it.polimi.distsys.paxos.utils.ThreadUtil;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

public class Main {
    private static final int DEFAULT_TIMEOUT = 10000;
    private static final int MAX_SLEEP_BETWEEN_TESTS = 1000;
    private static DHT dht;
    private static RandomString randomString;
    private static Random random;
    private static int numThreads = 10;
    private static int numTests = 10;

    public static void main(String [] args) {
        try {
            dht = new DHTImpl("localhost", 2019, "nodes.json");
            randomString = new RandomString(10);
            random = new Random();
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }

        for(int i=0; i < numThreads; i++) {
            ThreadUtil.getExecutorService().submit(Main::test);
        }
    }

    private static void test() {
        for(int i = 0; i < numTests; i++) {
            try {
                putGetRemove();
                Thread.sleep(random.nextInt(MAX_SLEEP_BETWEEN_TESTS));
            } catch (Exception e) {
                System.out.println("ERROR RUNNING TESTS.");
                e.printStackTrace();
            }
        }
    }

    private static void putGetRemove() throws Exception {
        String threadName = Thread.currentThread().getName();
        String k = randomString.nextString();
        String v = randomString.nextString();

        System.out.println("[" + Thread.currentThread().getName() + "] Starting test.");
        System.out.println("[" + Thread.currentThread().getName() + "] Invoking put " + k + ":" + v);
        CompletableFuture<Boolean> a = dht.put(k, v, DEFAULT_TIMEOUT);

        a.exceptionally(ex -> {
            System.out.println("[" + threadName + "] Exception: " + ex.getMessage());
            ex.printStackTrace();
            return false;
        });

        if(!a.get()) {
            System.out.println("[" + threadName + "] Put failed, quit.");
            return;
        }

        System.out.println("[" + threadName + "] Put ok.");
        System.out.println("[" + threadName + "] Invoking get " + k);
        CompletableFuture<String> b = dht.get(k, DEFAULT_TIMEOUT);

        b.exceptionally(ex -> {
            System.out.println("[" + threadName + "] Exception: " + ex.getMessage());
            ex.printStackTrace();
            return null;
        });

        if(!v.equals(b.get())) {
            System.out.println("[" + threadName + "] Get failed, quit.");
            return;
        }

        System.out.println("[" + threadName + "] Get ok.");
        System.out.println("[" + threadName + "] Invoking remove " + k);
        CompletableFuture<Boolean> c = dht.remove(k, DEFAULT_TIMEOUT);

        c.exceptionally(ex -> {
            System.out.println("[" + threadName + "] Exception: " + ex.getMessage());
            ex.printStackTrace();
            return false;
        });

        if(!c.get()) {
            System.out.println("[" + threadName + "] Remove failed, quit.");

        } else {
            System.out.println("[" + threadName + "] Remove ok.");
            System.out.println("[" + threadName + "] Test ended.");
        }
    }
}
