package edgelab.retryFreeDB.repo.storage;

import lombok.Getter;

import java.sql.Connection;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class DBTransaction {
    @Getter
    private String timestamp;
    @Getter
    private boolean abort = false;
    @Getter
    private Connection connection;


    @Getter
    private long waitingTime;
    private long previousWaitStart = -1;

    @Getter
    private final Set<String> resources = ConcurrentHashMap.newKeySet();
    private final AtomicInteger commitSemaphore;
    public DBTransaction(String timestamp,Connection connection) {

        this.timestamp = timestamp;
        this.abort = false;
        this.connection = connection;
        commitSemaphore = new AtomicInteger(0);
        this.waitingTime = 0;
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

    public void incCommitSemaphore() {
        commitSemaphore.incrementAndGet();
    }
    public void decCommitSemaphore() {
        commitSemaphore.decrementAndGet();
    }

    public Long getTimestampValue() {
        return Long.parseLong(timestamp);
    }


    private boolean canCommit() {
        return commitSemaphore.get() == 0;
    }

    public void waitForCommit() throws Exception {
        while (!this.canCommit()) {
            if (isAbort())
                throw new Exception("transaction already aborted");
            Thread.sleep(1);

        }
    }

    public void startWaiting() {
        previousWaitStart = System.currentTimeMillis();
    }


    public void wakesUp() {
        if (previousWaitStart != -1)
            this.waitingTime += (System.currentTimeMillis() - previousWaitStart);
    }
}
