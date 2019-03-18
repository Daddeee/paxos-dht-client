package it.polimi.distsys.dht;

import java.util.concurrent.CompletableFuture;

public interface DHT {
    CompletableFuture<String> get(String key, long timeout);
    CompletableFuture<Boolean> put(String key, String value, long timeout);
    CompletableFuture<Boolean> remove(String key, long timeout);
}
