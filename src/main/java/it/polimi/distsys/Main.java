package it.polimi.distsys;

import it.polimi.distsys.dht.DHT;
import it.polimi.distsys.dht.DHTImpl;
import it.polimi.distsys.paxos.utils.ThreadUtil;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public class Main {
    private static final int DEFAULT_TIMEOUT = 10000;
    private static final int MAX_SLEEP_BETWEEN_TESTS = 5000;
    private static final int MAX_STRING_LENGTH = 5;
    private static DHT dht;
    private static RandomString randomString;
    private static Random random;
    private static int numKeys = 3;
    private static int threadPerKeys = 2;
    private static int iterationPerThread = 20;
    private static Function<String, String> [] foos;

    public static void main(String [] args) {
        try {
            dht = new DHTImpl("localhost", 2019, "nodes.json");
            randomString = new RandomString(10);
            random = new Random();
            initFoos();
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }

        for(int i = 0; i < numKeys; i++) {
            final String key = randomString.nextString();
            for(int j = 0; j < threadPerKeys; j++) {
                ThreadUtil.getExecutorService().submit(() -> test(key));
            }
        }
    }

    private static void test(String key) {
        //for(int i = 0; i < iterationPerThread; i++) {
        while(true) {
            try {
                Thread.sleep(random.nextInt(MAX_SLEEP_BETWEEN_TESTS));
                getOperationPut(key);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static void initFoos() {
        foos = new Function[3];
        foos[0] = Main::append;
        foos[1] = Main::prepend;
        foos[2] = Main::substring;
    }

    private static void getOperationPut(String key) throws Exception {
        String threadName = Thread.currentThread().getName();
        System.out.println("[" + threadName + "] Starting test.");
        System.out.println("[" + threadName + "] Invoking get " + key);

        CompletableFuture<String> get = dht.get(key, DEFAULT_TIMEOUT);
        get.exceptionally(ex -> {
            System.out.println("[" + threadName + "] Get failed: " + ex.getMessage());
            ex.printStackTrace();
            return null;
        });

        String res = get.get();
        System.out.println("[" + threadName + "] Get ok: " + res);
        String newRes = getRandomOperation().apply(res);
        System.out.println("[" + threadName + "] New string: " + newRes);

        CompletableFuture<Boolean> put = dht.put(key, newRes, DEFAULT_TIMEOUT);
        put.exceptionally(ex -> {
            System.out.println("[" + threadName + "] Put failed: " + ex.getMessage());
            ex.printStackTrace();
            return null;
        });

        if(put.get() == null) {
            System.out.println("[" + threadName + "] Exiting.");
        } else {
            System.out.println("[" + threadName + "] Put ok.");
        }
        System.out.println("[" + threadName + "] Test ended.");
    }

    private static Function<String, String> getRandomOperation() {
        return foos[random.nextInt(foos.length)];
    }

    private static String append(String s) {
        if(s == null) return "" + randomString.nextString(random.nextInt(MAX_STRING_LENGTH));
        return s.concat("" + randomString.nextString(random.nextInt(MAX_STRING_LENGTH)));
    }

    private static String prepend(String s) {
        if(s == null) return  "" + randomString.nextString(random.nextInt(MAX_STRING_LENGTH));
        return ("" + randomString.nextString(random.nextInt(MAX_STRING_LENGTH))).concat(s);
    }

    private static String substring(String s) {
        if(s == null || s.length() == 0) return "";
        int beginIndex = random.nextInt(s.length());
        int endIndex = beginIndex + random.nextInt(s.length() - beginIndex);
        return s.substring(beginIndex, endIndex);
    }
}
