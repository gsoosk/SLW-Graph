package edgelab.retryFreeDB.repo.storage;

import lombok.Getter;

import java.sql.Connection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class DBTransaction {
    @Getter
    private String timestamp;
    private boolean abort = false;
    @Getter
    private Connection connection;

    @Getter
    private final Set<String> resources = ConcurrentHashMap.newKeySet();
    public DBTransaction(String timestamp,Connection connection) {

        this.timestamp = timestamp;
        this.abort = false;
        this.connection = connection;
    }

    @Override
    public String toString() {
        return timestamp;
    }

    public void setAbort() {
        this.abort = true;
    }


    public void addResource(String resource) {
        resources.add(resource);
    }

    public void clearResources() {
        resources.clear();;
    }

    public void removeResource(String resource) {
        resources.remove(resource);
    }
}
